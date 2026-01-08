#!/usr/bin/env python3
"""Ежедневная загрузка данных из Яндекс.Вебмастер API."""
import sys
import os
import logging
from datetime import datetime, timedelta

# Добавляем родительскую директорию в путь для импорта модулей
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from src.services.date_manager import DateManager
from src.api.webmaster_client import WebmasterClient
from src.core.webmaster_loader import WebmasterDataLoader

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/daily_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Основная функция."""
    logger.info('=' * 70)
    logger.info('ЕЖЕДНЕВНАЯ ЗАГРУЗКА ДАННЫХ ЯНДЕКС.ВЕБМАСТЕР')
    logger.info('=' * 70)
    
    try:
        # Инициализация компонентов
        logger.info('Инициализация компонентов...')
        client = WebmasterClient()
        date_manager = DateManager(client)
        data_loader = WebmasterDataLoader()
        
        # 1. Проверяем недостающие даты
        logger.info('Поиск недостающих дат...')
        missing_dates = date_manager.get_missing_dates()
        
        if not missing_dates:
            logger.info('✅ Все данные за последние 20 дней уже загружены')
            print('\n📊 ИТОГО: Нет новых данных для загрузки')
            return 0
        
        logger.info(f'Найдено недостающих дат: {len(missing_dates)}')
        print(f'\n📅 Найдено дат для загрузки: {len(missing_dates)}')
        print(f'Даты: {missing_dates}')
        
        # 2. Загружаем данные за каждую недостающую дату
        total_loaded = 0
        for i, date_str in enumerate(missing_dates, 1):
            logger.info(f'[{i}/{len(missing_dates)}] Загрузка данных за {date_str}...')
            print(f'\n[{i}/{len(missing_dates)}] 📥 Загрузка {date_str}...')
            
            try:
                loaded_count = data_loader.load_date(date_str)
                total_loaded += loaded_count
                logger.info(f'Загружено {loaded_count} записей за {date_str}')
                print(f'   ✅ Загружено: {loaded_count} записей')
                
            except Exception as e:
                logger.error(f'Ошибка загрузки данных за {date_str}: {e}')
                print(f'   ❌ Ошибка: {e}')
                continue
        
        # 3. Финальный отчет
        print('\n' + '=' * 70)
        print('📊 ФИНАЛЬНЫЙ ОТЧЕТ:')
        print(f'   - Обработано дат: {len(missing_dates)}')
        print(f'   - Загружено записей: {total_loaded}')
        print('=' * 70)
        
        logger.info(f'Загрузка завершена. Всего загружено: {total_loaded} записей')
        return total_loaded
        
    except Exception as e:
        logger.error(f'Критическая ошибка: {e}', exc_info=True)
        print(f'\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}')
        return -1

if __name__ == '__main__':
    result = main()
    sys.exit(0 if result >= 0 else 1)
