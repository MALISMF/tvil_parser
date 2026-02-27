import csv
import sys
import os
import logging
from pathlib import Path
from datetime import date, datetime
from zoneinfo import ZoneInfo
from collections import defaultdict
from log_config import setup_logging, get_log_file_path, send_telegram_summary

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)


def _run_date():
    tz_name = os.environ.get("RUN_TZ", "Asia/Irkutsk")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        return date.today()


def generate_statistics(run_date=None):
    """Генерирует статистику по отелям на основе данных из CSV файлов.
    run_date — дата сбора (по умолчанию сегодня по RUN_TZ)."""
    
    current_dir = Path(__file__).parent
    if run_date is None:
        run_date = _run_date()
    date_str = run_date.isoformat()
    hotels_csv = current_dir / 'tables' / 'hotels' / f'{date_str}.csv'
    rooms_csv = current_dir / 'tables' / 'rooms' / f'{date_str}.csv'
    output_csv = current_dir / 'tables' / 'statistics' / f'{date_str}.csv'
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Читаем данные об отелях
    hotels_data = {}
    try:
        with open(hotels_csv, 'r', encoding='utf-8-sig', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tvil_hotel_id = row.get('tvil_hotel_id', '')
                if tvil_hotel_id:
                    hotels_data[tvil_hotel_id] = {
                        'name': row.get('name', ''),
                        'rooms_number': row.get('rooms_number', '')
                    }
    except Exception as e:
        logger.error("Ошибка при чтении %s: %s", hotels_csv, e)
        return
    
    # Собираем статистику по номерам
    rooms_stats = defaultdict(lambda: {
        'free_rooms_amount': 0,
        'min_price': None,
        'max_capacity': 0,  # суммарная вместимость всех свободных номеров
    })
    
    try:
        with open(rooms_csv, 'r', encoding='utf-8-sig', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tvil_hotel_id = row.get('tvil_hotel_id', '')
                if not tvil_hotel_id:
                    continue
                
                free_rooms = row.get('free_rooms', '')
                free_rooms_value = 0
                try:
                    free_rooms_value = int(free_rooms) if free_rooms else 0
                    rooms_stats[tvil_hotel_id]['free_rooms_amount'] += free_rooms_value
                except (ValueError, TypeError):
                    pass

                room_cap_str = row.get('room_capacity', '')
                try:
                    capacity_per_room = int(room_cap_str) if room_cap_str else 0
                    if capacity_per_room > 0 and free_rooms_value > 0:
                        rooms_stats[tvil_hotel_id]['max_capacity'] += free_rooms_value * capacity_per_room
                except (ValueError, TypeError):
                    pass
                
                # Минимальная цена только среди номеров, где есть свободные (free_rooms > 0)
                price = row.get('price', '')
                if price and free_rooms_value > 0:
                    try:
                        price_value = float(price)
                        if price_value > 0:
                            current_min = rooms_stats[tvil_hotel_id]['min_price']
                            if current_min is None or price_value < current_min:
                                rooms_stats[tvil_hotel_id]['min_price'] = price_value
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        logger.error("Ошибка при чтении %s: %s", rooms_csv, e)
        return
    
    collection_date = run_date.strftime('%Y-%m-%d')
    
    statistics = []
    
    # Обрабатываем отели из hotels_csv
    for tvil_hotel_id, hotel_info in hotels_data.items():
        rooms_num_str = hotel_info.get('rooms_number', '')
        try:
            rooms_num = int(rooms_num_str) if rooms_num_str else 0
        except (ValueError, TypeError):
            rooms_num = 0
        
        stats = rooms_stats.get(tvil_hotel_id, {})
        free_rooms_amount = stats.get('free_rooms_amount', 0)
        min_price = stats.get('min_price')
        max_capacity = stats.get('max_capacity', 0)
        
        # Вычисляем процент доступных номеров
        if rooms_num > 0:
            available_rooms_percent = round((free_rooms_amount / rooms_num) * 100, 2)
        else:
            available_rooms_percent = 0.0
        
        # Форматируем минимальную цену
        min_price_str = f"{min_price:.2f}" if min_price is not None else ""
        
        statistics.append({
            'tvil_hotel_id': tvil_hotel_id,
            'name': hotel_info.get('name', ''),
            'rooms_num': str(rooms_num),
            'free_rooms_amount': str(free_rooms_amount),
            'max_capacity': str(max_capacity),
            'available_rooms_percent': str(available_rooms_percent),
            'date': collection_date,
            'min_price': min_price_str
        })
    
    # Сохраняем в CSV
    fieldnames = [
        'tvil_hotel_id',
        'name',
        'rooms_num',
        'free_rooms_amount',
        'max_capacity',
        'available_rooms_percent',
        'date',
        'min_price'
    ]
    
    try:
        with open(output_csv, 'w', encoding='utf-8-sig', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(statistics)
        logger.info("Статистика сохранена в %s", output_csv)
        logger.info("Обработано %s отелей", len(statistics))
        return len(statistics)
    except Exception as e:
        logger.error("Ошибка при сохранении статистики: %s", e)
        return None


if __name__ == "__main__":
    run_date = _run_date()
    setup_logging(log_file=get_log_file_path(run_date))

    count = generate_statistics(run_date)
    send_telegram_summary(f"Tvil: статистика сформирована. Отелей в отчёте: {count or 0}. Дата: {run_date}.")
