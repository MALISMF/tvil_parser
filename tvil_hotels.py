from playwright.sync_api import sync_playwright
import time
import sys
import os
import csv
import logging
from pathlib import Path
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

from log_config import setup_logging, get_log_file_path, send_telegram_summary

# Настройка stdout для корректного вывода Юникода и сброс буфера в CI
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
sys.stdout.reconfigure(line_buffering=True)

logger = logging.getLogger(__name__)


def _run_date():
    """Дата запуска по RUN_TZ (по умолчанию Asia/Irkutsk)."""
    tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return date.today()


def _is_ci():
    return os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("CI") == "true"


class TvilHotelsDailyParser:
    def __init__(self):
        self.api_url = "https://tvil.ru/api/entities"
        self.base_url = "https://tvil.ru"
        self.city_path = "/city/irkutskaya-oblast/"
        self.all_hotels = []
        self.current_dir = Path(__file__).parent
        self._meta_total = None  # общее количество отелей по запросу
        self._seen_ids = set()
        self.ci = _is_ci()
        if self.ci:
            logger.info("Режим CI: увеличенные таймауты.")

    def _build_page_url(self, arrival_date, departure_date, page_num=1):
        """Строит URL страницы поиска с датами и 1 гостем
        Страница 1: /city/irkutskaya-oblast/?gp[entity_type][0]=1&...
        Страница 2+: /city/irkutskaya-oblast/page2/?gp[entity_type][0]=1&...
        """
        params = urlencode({
            "gp[entity_type][0]": "1",
            "o[arrival]": arrival_date.strftime("%Y-%m-%d"),
            "o[departure]": departure_date.strftime("%Y-%m-%d"),
            "o[maleCount]": "1",
        })
        if page_num == 1:
            return f"{self.base_url}{self.city_path}?{params}"
        return f"{self.base_url}{self.city_path}page{page_num}/?{params}"

    def _setup_response_interceptor(self, page):
        """Перехват ответов от API Tvil (/api/entities)"""
        def handle_response(response):
            if (self.api_url in response.url
                    and response.status == 200
                    and response.request.method == "GET"
                    and "entity_type" in response.url):
                try:
                    if "json" in response.headers.get("content-type", "").lower():
                        json_data = response.json()
                        if not isinstance(json_data, dict) or "data" not in json_data:
                            return
                        if self._meta_total is None:
                            meta = json_data.get("meta", {})
                            t = (meta.get("total") or meta.get("count")
                                 or meta.get("totalCount"))
                            if t is not None:
                                self._meta_total = int(t)
                                logger.info("meta.total = %s", self._meta_total)
                        extracted = self._extract_hotels_from_json(json_data)
                        if extracted:
                            self.all_hotels.extend(extracted)
                            logger.info(
                                "Перехвачено %s отелей. Всего: %s",
                                len(extracted), len(self.all_hotels),
                            )
                except Exception as e:
                    msg = str(e)
                    if ("No resource with given identifier" not in msg
                            and "getResponseBody" not in msg
                            and "Target page, context or browser has been closed" not in msg):
                        logger.error("Ошибка разбора ответа API: %s", e)

        page.on("response", handle_response)

    def _wait_for_hotels(self, hotels_before, timeout=20):
        """Ждёт появления новых отелей"""
        start = time.time()
        while len(self.all_hotels) == hotels_before and (time.time() - start) < timeout:
            time.sleep(0.3)

    def get_all_hotels_list(self):
        """Основная функция для парсинга списка отелей на следующие 2 дня"""
        logger.info("Запуск парсера отелей...")
        today = _run_date()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)

        logger.info("Даты бронирования: %s - %s", arrival_date.strftime('%d.%m.%Y'), departure_date.strftime('%d.%m.%Y'))

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale='ru-RU',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            self._setup_response_interceptor(page)

            goto_timeout  = 90000 if self.ci else 90000
            wait_timeout  = 60    if self.ci else 60

            page_num = 1
            while True:
                url = self._build_page_url(arrival_date, departure_date, page_num)
                logger.info("--- Страница %s --- %s", page_num, url)

                hotels_before = len(self.all_hotels)

                try:
                    page.goto(url, wait_until='networkidle', timeout=goto_timeout)
                except Exception as e:
                    logger.warning("[Страница %s] goto: %s", page_num, e)

                if self.ci:
                    try:
                        page.wait_for_load_state("networkidle", timeout=45000)
                    except Exception:
                        pass

                self._wait_for_hotels(hotels_before, timeout=wait_timeout)
                time.sleep(1)

                if len(self.all_hotels) == hotels_before:
                    logger.info("Страница %s не дала новых отелей. Конец.", page_num)
                    break

                if self._meta_total is not None and len(self.all_hotels) >= self._meta_total:
                    logger.info(
                        "Набрано %s, meta.total = %s",
                        len(self.all_hotels), self._meta_total,
                    )
                    break

                page_num += 1

            browser.close()

        if self.all_hotels:
            self._deduplicate_hotels()
            if self._meta_total is not None and len(self.all_hotels) > self._meta_total:
                logger.error(
                    "Собрано %s отелей, что больше meta.total (%s) — возможна ошибка пагинации.",
                    len(self.all_hotels), self._meta_total,
                )
            self._save_to_csv()
            logger.info("Парсинг завершён. Всего обработано %s отелей.", len(self.all_hotels))
        else:
            logger.warning("Не удалось извлечь данные об отелях.")

        return self.all_hotels

    def _extract_hotels_from_json(self, json_data):
        """Извлечение отелей из JSON ответа от API Tvil"""
        hotels_list = []

        if not json_data or "data" not in json_data:
            return hotels_list
            
        data_array = json_data["data"]
        if not isinstance(data_array, list):
            return hotels_list

        for hotel in data_array:
            try:
                attributes = hotel.get("attributes", {})
                links = hotel.get("links", {})

                if not attributes:
                    continue

                hotel_id = hotel.get("id", "")
                title = attributes.get("title", "")

                if not hotel_id or not title:
                    continue

                public_link = links.get("public", "")
                if public_link:
                    if public_link.startswith('/'):
                        url = f"{self.base_url}{public_link}"
                    elif public_link.startswith('http'):
                        url = public_link
                    else:
                        url = f"{self.base_url}/{public_link}"
                else:
                    url = ""

                hotel_data = {
                    "city": attributes.get("city_address", ""),
                    "tvil_hotel_id": hotel_id,
                    "name": title,
                    "address": attributes.get("address", ""),
                    "latitude": str(attributes.get("latitude", "")),
                    "longitude": str(attributes.get("longitude", "")),
                    "url": url,
                    "rooms_number": str(attributes.get("rooms_total", "")),
                }

                hotels_list.append(hotel_data)
            except Exception:
                continue

        return hotels_list

    def _deduplicate_hotels(self):
        """Удаление дубликатов по tvil_hotel_id, порядок сохраняется."""
        seen = set()
        unique = []
        for h in self.all_hotels:
            key = h.get("tvil_hotel_id") or ""
            if key not in seen:
                seen.add(key)
                unique.append(h)
        removed = len(self.all_hotels) - len(unique)
        if removed:
            logger.info("Убрано дубликатов: %s. Уникальных отелей: %s", removed, len(unique))
        self.all_hotels = unique

    def _save_to_csv(self):
        """Сохранение списка отелей в CSV файл (daily/hotels/YYYY-MM-DD.csv)."""
        if not self.all_hotels:
            return

        run_date = _run_date()
        output_dir = self.current_dir / 'daily' / 'hotels'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'

        fieldnames = ['city', 'tvil_hotel_id', 'name', 'address', 'latitude', 'longitude', 'url', 'rooms_number']

        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(
                    csv_file, fieldnames=fieldnames,
                    delimiter=',', quoting=csv.QUOTE_MINIMAL,
                )
                writer.writeheader()
                for hotel in self.all_hotels:
                    writer.writerow(hotel)
            logger.info("Сохранено %s отелей в %s", len(self.all_hotels), csv_filename)
        except Exception as e:
            logger.error("Ошибка при сохранении CSV: %s", e)


class TvilHotelsCatalog:
    """Ведёт накопленный каталог всех спарсенных отелей Tvil за всё время.
    При каждом запуске — добавляет новые отели и обновляет last_seen_date у существующих.
    Файл: catalog/hotels.csv"""

    FIELDNAMES = [
        'tvil_hotel_id', 'name', 'city', 'address', 'latitude', 'longitude', 'url', 'rooms_number',
        'first_seen_date', 'last_seen_date',
    ]

    def __init__(self):
        self.current_dir = Path(__file__).parent
        self.catalog_path = self.current_dir / 'catalog' / 'hotels.csv'

    def _load_existing(self):
        """Читает текущий каталог. Возвращает dict {tvil_hotel_id: row}."""
        existing = {}
        if not self.catalog_path.exists():
            return existing
        try:
            with open(self.catalog_path, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    hotel_id = row.get('tvil_hotel_id', '')
                    if hotel_id:
                        existing[hotel_id] = row
        except Exception as e:
            logger.error("Ошибка при чтении каталога %s: %s", self.catalog_path, e)
        return existing

    def _save(self, hotels: dict):
        """Сохраняет каталог на диск."""
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(self.catalog_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES, quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(hotels.values())
            logger.info("Каталог сохранён: %s отелей → %s", len(hotels), self.catalog_path)
        except Exception as e:
            logger.error("Ошибка при сохранении каталога: %s", e)

    def update(self, parsed_hotels: list):
        """Обновляет каталог на основе свежего списка отелей.
        - Новые отели добавляются с first_seen_date = сегодня.
        - Существующие — обновляют поля и last_seen_date.
        Возвращает (всего в каталоге, новых добавлено)."""
        today = _run_date().isoformat()
        existing = self._load_existing()

        new_count = 0
        for hotel in parsed_hotels:
            hotel_id = str(hotel.get('tvil_hotel_id', ''))
            if not hotel_id:
                continue

            if hotel_id in existing:
                existing[hotel_id].update({
                    'name':           hotel.get('name', existing[hotel_id]['name']),
                    'city':           hotel.get('city', existing[hotel_id]['city']),
                    'address':        hotel.get('address', existing[hotel_id]['address']),
                    'latitude':       hotel.get('latitude', existing[hotel_id].get('latitude', '')),
                    'longitude':      hotel.get('longitude', existing[hotel_id].get('longitude', '')),
                    'url':            hotel.get('url', existing[hotel_id]['url']),
                    'rooms_number':   hotel.get('rooms_number', existing[hotel_id]['rooms_number']),
                    'last_seen_date': today,
                })
            else:
                existing[hotel_id] = {
                    'tvil_hotel_id':   hotel_id,
                    'name':            hotel.get('name', ''),
                    'city':            hotel.get('city', ''),
                    'address':         hotel.get('address', ''),
                    'latitude':        hotel.get('latitude', ''),
                    'longitude':       hotel.get('longitude', ''),
                    'url':             hotel.get('url', ''),
                    'rooms_number':    hotel.get('rooms_number', ''),
                    'first_seen_date': today,
                    'last_seen_date':  today,
                }
                new_count += 1

        self._save(existing)
        logger.info("Каталог обновлён: всего %s, новых %s", len(existing), new_count)
        return len(existing), new_count


if __name__ == "__main__":
    run_date = _run_date()
    setup_logging(log_file=get_log_file_path(run_date))

    parser = TvilHotelsDailyParser()
    result = parser.get_all_hotels_list()

    catalog = TvilHotelsCatalog()
    total, new_count = catalog.update(result)

    send_telegram_summary(
        f"Tvil: парсинг отелей завершён. Отелей: {len(result)}. "
        f"Каталог: {total} всего, {new_count} новых. Дата: {run_date}."
    )