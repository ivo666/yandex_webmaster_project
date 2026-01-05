"""Main data collector for Yandex Webmaster."""
import logging
from typing import Optional, List
from datetime import datetime

from config.settings import settings
from api.webmaster_client import WebmasterClient
from services.date_manager import DateManager
from services.data_loader import DataLoader
from models.database import create_tables


class WebmasterCollector:
    """Main collector for Yandex Webmaster data."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Инициализируем компоненты
        self.client = WebmasterClient()
        self.date_manager = DateManager(self.client)
        self.data_loader = DataLoader(self.client)
        
        # Настройка логирования
        logging.basicConfig(
            level=getattr(logging, settings.LOG_LEVEL),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def initialize_database(self):
        """Initialize database tables."""
        self.logger.info("Initializing database...")
        try:
            create_tables()
            self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def collect_for_date(self, target_date: str) -> int:
        """Collect data for specific date."""
        self.logger.info(f"Starting collection for date: {target_date}")
        
        try:
            # Загружаем данные за дату
            records_count = self.data_loader.load_data_for_date(target_date)
            
            if records_count > 0:
                self.logger.info(f"Successfully collected {records_count} records for {target_date}")
            else:
                self.logger.warning(f"No new records collected for {target_date}")
            
            return records_count
            
        except Exception as e:
            self.logger.error(f"Failed to collect data for {target_date}: {e}")
            return 0
    
    def collect_missing_data(self) -> int:
        """Collect missing data for recent dates."""
        self.logger.info("Starting collection of missing data...")
        
        # Получаем недостающие даты
        missing_dates = self.date_manager.get_missing_dates()
        
        if not missing_dates:
            self.logger.info("No missing dates found")
            return 0
        
        self.logger.info(f"Found {len(missing_dates)} missing dates: {missing_dates}")
        
        total_records = 0
        for date_str in missing_dates:
            records = self.collect_for_date(date_str)
            total_records += records
        
        self.logger.info(f"Total collected: {total_records} records from {len(missing_dates)} dates")
        return total_records
    
    def collect_for_period(self, start_date: str, end_date: str) -> int:
        """Collect data for specific period."""
        self.logger.info(f"Collecting data for period: {start_date} to {end_date}")
        
        # Генерируем даты периода
        from datetime import datetime as dt, timedelta
        
        start = dt.strptime(start_date, '%Y-%m-%d').date()
        end = dt.strptime(end_date, '%Y-%m-%d').date()
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        total_records = 0
        for date_str in dates:
            records = self.collect_for_date(date_str)
            total_records += records
        
        self.logger.info(f"Period collection completed: {total_records} records")
        return total_records
    
    def collect_yesterday(self) -> int:
        """Collect data for yesterday."""
        from datetime import datetime as dt, timedelta
        
        yesterday = dt.now().date() - timedelta(days=1)
        date_str = yesterday.strftime('%Y-%m-%d')
        
        self.logger.info(f"Collecting yesterday's data: {date_str}")
        return self.collect_for_date(date_str)


def main():
    """Main entry point."""
    import sys
    
    collector = WebmasterCollector()
    
    # Проверяем аргументы командной строки
    if len(sys.argv) == 1:
        # Без аргументов - собираем недостающие данные
        collector.collect_missing_data()
    elif len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg == '--yesterday' or arg == '-y':
            collector.collect_yesterday()
        elif arg == '--init' or arg == '-i':
            collector.initialize_database()
        else:
            # Предполагаем что это дата
            collector.collect_for_date(arg)
    elif len(sys.argv) == 3:
        # Период
        collector.collect_for_period(sys.argv[1], sys.argv[2])
    else:
        print("Usage:")
        print("  python -m core.collector                    # Collect missing data")
        print("  python -m core.collector --yesterday        # Collect yesterday's data")
        print("  python -m core.collector YYYY-MM-DD         # Collect specific date")
        print("  python -m core.collector YYYY-MM-DD YYYY-MM-DD  # Collect period")
        print("  python -m core.collector --init             # Initialize database")


if __name__ == "__main__":
    main()
