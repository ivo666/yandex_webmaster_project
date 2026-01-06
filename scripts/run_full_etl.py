#!/usr/bin/env python3
"""Скрипт для полной обработки ETL"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor import WebmasterETLProcessor

def main():
    print("🚀 Запуск ПОЛНОЙ обработки ETL...")
    
    db_manager = DatabaseManager()
    processor = WebmasterETLProcessor(db_manager)
    
    result = processor.process_full_reload()
    
    print(f"✅ Полная обработка завершена!")
    print(f"📊 Результат:")
    print(f"   Обработано записей: {result['aggregated_inserted']}")
    print(f"   Создано позиций: {result['positions_created']}")
    print(f"   Создано кликов: {result['clicks_created']}")
    print(f"   Ошибок: {result['errors']}")

if __name__ == "__main__":
    main()
