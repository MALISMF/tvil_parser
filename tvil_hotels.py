from playwright.sync_api import sync_playwright
import time
import sys
import os
import csv
import json
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


class TvilHotelsDailyParser:
    def __init__(self):
        self.api_url = "https://tvil.ru/api/entities"
        self.base_url = "https://tvil.ru"
        self.all_hotels = []
        self.current_dir = Path(__file__).parent
    
    def get_all_hotels_list(self):
        """Основная функция для парсинга списка отелей на следующие 2 дня"""
        logger.info("Запуск парсера отелей...")
        today = _run_date()
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        
        logger.info("Даты бронирования: %s - %s", arrival_date.strftime('%d.%m.%Y'), departure_date.strftime('%d.%m.%Y'))
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            logger.info("Перехожу на страницу tvil.ru...")
            page.goto('https://tvil.ru/city/irkutskaya-oblast/?gp%5Bentity_type%5D%5B0%5D=1', wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(3000)
            
            # Получаем первый URL для запроса
            first_url = self._build_api_url(arrival_date, departure_date)
            
            # Парсим все страницы с пагинацией
            self._parse_all_pages_with_pagination(page, first_url)
            
            browser.close()
        
        if self.all_hotels:
            self._save_to_csv()
            logger.info("Парсинг завершён. Всего обработано %s отелей.", len(self.all_hotels))
        else:
            logger.warning("Не удалось извлечь данные об отелях.")
        
        return self.all_hotels
    
    def _build_api_url(self, arrival_date, departure_date):
        """Построение URL для API запроса"""
        params = {
            'page[limit]': '20',
            'page[offset]': '0',
            'include': 'params,child_params,photos_t2,photos_t1,tooltip,services,inflect,characteristics',
            'filter[generalParam][entity_type][]': '1',
            'filter[type]': '',
            'filter[geo]': '251',
            'format[withNearEntities]': '1',
            'format[withBusyEntities]': '1',
            'format[withDisabledEntities]': '0',
            'order[arrival]': arrival_date.strftime('%Y-%m-%d'),
            'order[departure]': departure_date.strftime('%Y-%m-%d'),
            'order[male]': '1'
        }
        query_string = urlencode(params, doseq=True)
        return f"{self.api_url}?{query_string}"
    
    def _parse_all_pages_with_pagination(self, page, first_url):
        """Парсинг всех страниц с пагинацией"""
        current_url = first_url
        page_number = 1
        
        while current_url:
            logger.info("--- Страница %s ---", page_number)
            
            try:
                json_data = self._make_api_request(page, current_url)
                
                if not json_data:
                    logger.warning("Не удалось получить данные со страницы %s", page_number)
                    break
                
                # Извлекаем отели из JSON
                extracted_hotels = self._extract_hotels_from_json(json_data)
                if extracted_hotels:
                    self.all_hotels.extend(extracted_hotels)
                    logger.info("Извлечено %s отелей. Всего: %s", len(extracted_hotels), len(self.all_hotels))
                
                # Проверяем наличие следующей страницы
                links = json_data.get('links', {})
                next_url = links.get('next')
                
                if next_url:
                    # Преобразуем URL из /entities/ в /api/entities/ если нужно
                    if '/entities/' in next_url and '/api/entities/' not in next_url:
                        next_url = next_url.replace('/entities/', '/api/entities/')
                    
                    # Преобразуем относительный URL в полный
                    if next_url.startswith('/'):
                        current_url = f"{self.base_url}{next_url}"
                    elif next_url.startswith('http'):
                        current_url = next_url
                    else:
                        current_url = f"{self.base_url}/{next_url}"
                    page_number += 1
                    time.sleep(0.5)
                else:
                    logger.info("Достигнута последняя страница.")
                    break
                    
            except Exception as e:
                logger.error("Ошибка при парсинге страницы %s: %s", page_number, e)
                break
        
        logger.info("Всего собрано отелей со всех страниц: %s", len(self.all_hotels))
    
    def _make_api_request(self, page, api_url):
        """Выполнение API запроса через JavaScript на странице"""
        api_url_js = json.dumps(api_url)
        
        try:
            json_data = page.evaluate(f"""
                async () => {{
                    try {{
                        const response = await fetch({api_url_js}, {{
                            method: 'GET',
                            headers: {{
                                'accept': 'application/vnd.api+json',
                                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                                'cache-control': 'no-cache',
                                'derived-from': 'front_v3',
                                'pragma': 'no-cache',
                                'referer': 'https://tvil.ru/city/irkutskaya-oblast/?gp%5Bentity_type%5D%5B0%5D=1',
                                'sec-fetch-dest': 'empty',
                                'sec-fetch-mode': 'cors',
                                'sec-fetch-site': 'same-origin'
                            }},
                            credentials: 'include'
                        }});
                        
                        if (!response.ok) {{
                            return {{ error: true, status: response.status }};
                        }}
                        
                        return await response.json();
                    }} catch (error) {{
                        return {{ error: true, message: error.toString() }};
                    }}
                }}
            """)
            
            if json_data and isinstance(json_data, dict) and json_data.get('error'):
                return None
            
            return json_data
        except Exception as e:
            logger.error("Ошибка при выполнении API запроса: %s", e)
            return None
    
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
                
                # Получаем URL из links.public
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
                    "url": url,
                    "rooms_number": str(attributes.get("rooms_total", ""))
                }
                
                hotels_list.append(hotel_data)
            except Exception as e:
                continue
        
        return hotels_list
    
    def _save_to_csv(self):
        """Сохранение списка отелей в CSV файл"""
        if not self.all_hotels:
            return
        
        run_date = _run_date()
        output_dir = self.current_dir / 'tables' / 'hotels'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'
        
        fieldnames = ['city', 'tvil_hotel_id', 'name', 'address', 'url', 'rooms_number']
        
        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for hotel in self.all_hotels:
                    writer.writerow(hotel)
            logger.info("Сохранено %s отелей в %s", len(self.all_hotels), csv_filename)
        except Exception as e:
            logger.error("Ошибка при сохранении CSV: %s", e)


if __name__ == "__main__":
    run_date = _run_date()
    setup_logging(log_file=get_log_file_path(run_date))
    parser = TvilHotelsDailyParser()
    parser.get_all_hotels_list()
    send_telegram_summary(f"Tvil: парсер отелей завершён. Отелей: {len(parser.all_hotels)}. Дата: {run_date}.")
