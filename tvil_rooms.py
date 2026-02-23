from playwright.sync_api import sync_playwright
import time
import sys
import os
import csv
import json
import re
import logging
from pathlib import Path
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo

from log_config import setup_logging, get_log_file_path, send_telegram_summary

# Настройка stdout для корректного вывода Юникода
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


class TvilRoomsDailyParser:
    def __init__(self):
        self.api_url = "https://tvil.ru/api/entities"
        self.calculate_url = "https://tvil.ru/api/reserves/calculate"
        self.base_url = "https://tvil.ru"
        self.all_rooms = []
        self.current_dir = Path(__file__).parent
    
    def _read_hotels_from_csv(self, csv_path=None, run_date=None):
        """Читает список отелей из CSV файла (по умолчанию tables/hotels/{date}.csv)."""
        if csv_path is None:
            if run_date is None:
                run_date = _run_date()
            csv_path = self.current_dir / 'tables' / 'hotels' / f'{run_date.isoformat()}.csv'
        else:
            csv_path = Path(csv_path)
        
        hotels = []
        
        try:
            with open(csv_path, newline="", encoding="utf-8-sig") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=",")
                for row in reader:
                    hotels.append(row)
        except Exception as e:
            logger.error("Ошибка при чтении CSV %s: %s", csv_path, e)
        
        return hotels
    
    def _get_room_descriptions(self, page, hotel_id):
        """Получение описаний номеров через GET запрос к /api/entities"""
        api_url = f"{self.api_url}/{hotel_id}?include=photos_t1,photos_t2"
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
                return {}
            
            # Создаем словарь: object_id -> description
            descriptions = {}
            if json_data and 'included' in json_data:
                for item in json_data['included']:
                    if item.get('type') == 'photos':
                        attributes = item.get('attributes', {})
                        object_id = attributes.get('object_id')
                        description = attributes.get('description', '')
                        if object_id:
                            descriptions[str(object_id)] = description
            
            return descriptions
        except Exception as e:
            logger.error("Ошибка при получении описаний номеров: %s", e)
            return {}
    
    def _calculate_rooms(self, page, hotel_id, arrival_date, departure_date):
        """Получение данных о номерах через POST запрос к /api/reserves/calculate"""
        payload = {
            "data": {
                "type": "reserve_calculator",
                "attributes": {
                    "arrival": arrival_date.strftime('%Y-%m-%d'),
                    "departure": departure_date.strftime('%Y-%m-%d'),
                    "male": 1,
                    "female": 0,
                    "child_age": [],
                    "source": "reservation",
                    "isCalculationIncludingDisabledEntities": 1
                },
                "relationships": {
                    "entity": {
                        "data": {
                            "id": str(hotel_id),
                            "type": "entities"
                        }
                    }
                }
            },
            "meta": {}
        }
        
        payload_js = json.dumps(payload)
        calculate_url_js = json.dumps(self.calculate_url)
        
        try:
            json_data = page.evaluate(f"""
                async () => {{
                    try {{
                        const payload = {payload_js};
                        const response = await fetch({calculate_url_js}, {{
                            method: 'POST',
                            headers: {{
                                'accept': 'application/vnd.api+json',
                                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                                'cache-control': 'no-cache',
                                'content-type': 'application/vnd.api+json',
                                'derived-from': 'front_v3',
                                'origin': 'https://tvil.ru',
                                'pragma': 'no-cache',
                                'referer': 'https://tvil.ru/city/irkutsk/hotels/{hotel_id}/',
                                'sec-fetch-dest': 'empty',
                                'sec-fetch-mode': 'cors',
                                'sec-fetch-site': 'same-origin'
                            }},
                            credentials: 'include',
                            body: JSON.stringify(payload)
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
            logger.error("Ошибка при получении данных о номерах: %s", e)
            return None
    
    def _extract_all_rooms(self, text):
        """Извлечение общего количества номеров из текста 'Свободны X из Y'"""
        if not text:
            return 0
        
        match = re.search(r'Свободны \d+ из (\d+)', text)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, TypeError):
                return 0
        
        return 0
    
    def _parse_room_capacity(self, description):
        """Извлечение вместимости номера из description"""
        if not description:
            return ""
        
        match = re.search(r'(\d+)-местный', description)
        if match:
            return match.group(1)
        
        return ""
    
    def _extract_room_data(self, calculate_data, descriptions, hotel_id, hotel_url):
        """Извлечение данных о номерах из calculate.json и сопоставление с описаниями"""
        rooms_data = []
        
        if not calculate_data or 'data' not in calculate_data:
            return rooms_data
        
        data_array = calculate_data['data']
        
        if not isinstance(data_array, list) or len(data_array) == 0:
            return rooms_data
        
        # Первый элемент - отель, берем его id как tvil_hotel_id
        hotel_element = data_array[0]
        tvil_hotel_id = hotel_element.get('id', hotel_id)
        
        # Если только один элемент (только отель, без номеров)
        if len(data_array) == 1:
            # Записываем пустую строку
            rooms_data.append({
                "tvil_hotel_id": tvil_hotel_id,
                "room_name": "",
                "room_id": "",
                "free_rooms": "0",
                "all_rooms": "0",
                "room_capacity": "",
                "price": "0",
                "url": hotel_url
            })
            return rooms_data
        
        # Обрабатываем остальные элементы (номера)
        for room_item in data_array[1:]:
            try:
                room_id = room_item.get('id', '')
                attributes = room_item.get('attributes', {})
                
                # Получаем данные из calculate
                total_price = attributes.get('total_price')
                price = str(total_price) if total_price is not None else "0"
                
                rooms_data_attr = attributes.get('rooms_data', {})
                free_count = rooms_data_attr.get('free_count')
                free_rooms = str(free_count) if free_count is not None else "0"
                
                text = rooms_data_attr.get('text', '')
                all_rooms = str(self._extract_all_rooms(text))
                
                # Ищем описание по object_id (room_id)
                description = descriptions.get(str(room_id), '')
                room_name = description
                room_capacity = self._parse_room_capacity(description)
                
                rooms_data.append({
                    "tvil_hotel_id": tvil_hotel_id,
                    "room_name": room_name,
                    "room_id": room_id,
                    "free_rooms": free_rooms,
                    "all_rooms": all_rooms,
                    "room_capacity": room_capacity,
                    "price": price,
                    "url": hotel_url
                })
            except Exception as e:
                continue
        
        return rooms_data
    
    def _process_hotel(self, page, hotel_row, arrival_date, departure_date):
        """Обработка одного отеля"""
        hotel_id = hotel_row.get('tvil_hotel_id', '')
        hotel_url = hotel_row.get('url', '')
        hotel_name = hotel_row.get('name', hotel_id)
        
        if not hotel_id:
            logger.warning("Пропускаю %s: не найден tvil_hotel_id", hotel_name)
            return []
        
        logger.info("Обрабатываю отель: %s (ID: %s)", hotel_name, hotel_id)
        
        # Пытаемся перейти на страницу отеля для получения cookies (не критично, если не получится)
        try:
            page.goto(hotel_url, wait_until='domcontentloaded', timeout=15000)
            page.wait_for_timeout(1000)
        except Exception as e:
            logger.warning("Не удалось перейти на страницу отеля %s, продолжаю: %s", hotel_name, e)
        
        # Получаем описания номеров
        descriptions = self._get_room_descriptions(page, hotel_id)
        time.sleep(0.5)
        
        # Получаем данные о номерах
        calculate_data = self._calculate_rooms(page, hotel_id, arrival_date, departure_date)
        time.sleep(0.5)
        
        if not calculate_data:
            logger.warning("Не удалось получить данные для %s", hotel_name)
            return []
        
        # Извлекаем данные о номерах
        rooms_data = self._extract_room_data(calculate_data, descriptions, hotel_id, hotel_url)
        
        if rooms_data:
            rooms_count = len([r for r in rooms_data if r.get('room_id')])
            logger.info("Найдено %s номеров для %s", rooms_count, hotel_name)
        
        return rooms_data
    
    def get_all_rooms(self, csv_path=None, run_date=None):
        """Основная функция для парсинга номеров отелей из списка."""
        if run_date is None:
            run_date = _run_date()
        today = run_date
        arrival_date = today + timedelta(days=1)
        departure_date = today + timedelta(days=2)
        
        logger.info("Запуск парсера номеров...")
        logger.info("Даты бронирования: %s - %s", arrival_date.strftime('%d.%m.%Y'), departure_date.strftime('%d.%m.%Y'))
        
        # Читаем список отелей
        hotels = self._read_hotels_from_csv(csv_path, run_date)
        
        if not hotels:
            logger.error("Не удалось загрузить список отелей.")
            return []
        
        logger.info("Загружено %s отелей для обработки", len(hotels))
        
        # Обрабатываем каждый отель через Playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()
            
            logger.info("Перехожу на страницу tvil.ru...")
            page.goto('https://tvil.ru/city/irkutskaya-oblast/?gp%5Bentity_type%5D%5B0%5D=1', wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(2000)
            
            for idx, hotel_row in enumerate(hotels, 1):
                logger.info("--- Отель %s из %s ---", idx, len(hotels))
                rooms_data = self._process_hotel(page, hotel_row, arrival_date, departure_date)
                if rooms_data:
                    self.all_rooms.extend(rooms_data)
            
            browser.close()
        
        if self.all_rooms:
            self._save_to_csv(run_date)
            logger.info("Парсинг завершён. Всего обработано %s номеров.", len(self.all_rooms))
        else:
            logger.warning("Не удалось извлечь данные о номерах.")
        
        return self.all_rooms
    
    def _save_to_csv(self, run_date=None):
        """Сохранение данных номеров в CSV файл (tables/rooms/YYYY-MM-DD.csv)."""
        if not self.all_rooms:
            return
        if run_date is None:
            run_date = _run_date()
        
        output_dir = self.current_dir / 'tables' / 'rooms'
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_filename = output_dir / f'{run_date.isoformat()}.csv'
        
        fieldnames = ['tvil_hotel_id', 'room_name', 'room_id', 'free_rooms', 'all_rooms', 'room_capacity', 'price', 'url']
        
        try:
            with open(csv_filename, 'w', encoding='utf-8-sig', newline='') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                for room in self.all_rooms:
                    writer.writerow(room)
            logger.info("Сохранено %s номеров в %s", len(self.all_rooms), csv_filename)
        except Exception as e:
            logger.error("Ошибка при сохранении CSV: %s", e)


if __name__ == "__main__":
    run_date = _run_date()
    setup_logging(log_file=get_log_file_path(run_date))
    parser = TvilRoomsDailyParser()
    parser.get_all_rooms()
    send_telegram_summary(f"Tvil: парсер номеров завершён. Номеров: {len(parser.all_rooms)}. Дата: {run_date}.")
