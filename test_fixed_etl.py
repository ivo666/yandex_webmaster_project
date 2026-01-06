#!/usr/bin/env python3
"""Тест исправленного ETL"""
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor_fixed import WebmasterETLProcessorFixed

def main():
    print("🧪 Тест исправленного ETL процессора")
    
    db = DatabaseManager()
    processor = WebmasterETLProcessorFixed(db)
    
    # Проверяем статус
    status = processor.get_processing_status()
    print(f"\n📊 Статус:")
    print(f"   RDL записей: {status['unprocessed_count']}")
    print(f"   PPL пустая: {status['ppl_empty']}")
    print(f"   Нужна обработка: {status['needs_processing']}")
    
    if status['needs_processing']:
        print(f"\n⚡ Запуск обработки...")
        result = processor.process_incremental()
        print(f"\n✅ Результат:")
        print(f"   Обработано: {result['aggregated_inserted']} записей")
        print(f"   Позиций: {result['positions_created']}")
        print(f"   Кликов: {result['clicks_created']}")
        print(f"   Ошибок: {result['errors']}")

if __name__ == "__main__":
    main()
