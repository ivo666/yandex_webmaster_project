#!/usr/bin/env python3
"""Скрипт запуска ETL процесса"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor import WebmasterETLProcessor

def main():
    """Основная функция"""
    print("🚀 Запуск ETL процесса...")
    
    db_manager = DatabaseManager()
    processor = WebmasterETLProcessor(db_manager)
    
    # Проверяем статус
    status = processor.get_processing_status()
    print(f"📊 Статус:")
    print(f"   RDL последняя дата: {status['last_rdl_date']}")
    print(f"   PPL последняя дата: {status['last_ppl_date']}")
    print(f"   Необработанных: {status['unprocessed_count']}")
    
    if status['needs_processing']:
        print("⚡ Запуск инкрементальной обработки...")
        result = processor.process_incremental()
        print(f"✅ Результат: {result['aggregated_inserted']} записей обработано")
    else:
        print("✅ Данные актуальны, обработка не требуется")

if __name__ == "__main__":
    main()
