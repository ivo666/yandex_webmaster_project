#!/usr/bin/env python3
"""Ежедневный загрузчик данных Яндекс.Вебмастер."""
import sys
import os
import logging
from datetime import datetime, timedelta

# Добавляем путь
sys.path.insert(0, '/home/pf-server/yandex_webmaster_project/src')

# Настраиваем логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/pf-server/yandex_webmaster_project/logs/daily_loader.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Основная функция."""
    print('=' * 70)
    print('ЕЖЕДНЕВНАЯ ЗАГРУЗКА ДАННЫХ ЯНДЕКС.ВЕБМАСТЕР')
    print('=' * 70)
    
    try:
        from services.date_manager import DateManager
        from api.webmaster_client import WebmasterClient
        from core.webmaster_loader import WebmasterDataLoader
        
        logger.info('Инициализация компонентов...')
        client = WebmasterClient()
        date_manager = DateManager(client)
        loader = WebmasterDataLoader()
        
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
                loaded_count = loader.load_date(date_str)
                total_loaded += loaded_count
                
                logger.info(f'Загружено записей за {date_str}: {loaded_count}')
                print(f'   ✅ Загружено записей: {loaded_count}')
                
            except Exception as e:
                logger.error(f'Ошибка при загрузке {date_str}: {e}')
                print(f'   ❌ Ошибка: {e}')
                # Продолжаем со следующей датой
        
        # 3. Финальный отчет
        print('\n' + '=' * 70)
        print('📊 ФИНАЛЬНЫЙ ОТЧЕТ:')
        print(f'   - Обработано дат: {len(missing_dates)}')
        print(f'   - Загружено записей: {total_loaded}')
        
        if total_loaded > 0:
            print('✅ ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНА')
        else:
            print('⚠️  Нет новых данных для загрузки')
        
        return total_loaded
        
    except Exception as e:
        logger.error(f'Критическая ошибка: {e}')
        print(f'\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}')
        import traceback
        traceback.print_exc()
        return 0

if __name__ == "__main__":
    # Создаем папку для логов если её нет
    log_dir = '/home/pf-server/yandex_webmaster_project/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    result = main()
    sys.exit(0 if result >= 0 else 1)
