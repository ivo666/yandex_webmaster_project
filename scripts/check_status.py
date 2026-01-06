#!/usr/bin/env python3
"""Скрипт проверки статуса данных"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor_v2 import WebmasterETLProcessorV2

def main():
    print("📊 СТАТУС ДАННЫХ ЯНДЕКС.ВЕБМАСТЕР")
    print("=" * 40)
    
    db = DatabaseManager()
    processor = WebmasterETLProcessorV2(db)
    
    status = processor.get_processing_status()
    
    print(f"RDL (сырые данные):")
    print(f"  Последняя дата: {status['last_rdl_date']}")
    print(f"  Всего записей: {status['unprocessed_count'] + (status['last_ppl_date'] and 1 or 0)}")
    
    print(f"\nPPL (обработанные):")
    print(f"  Последняя дата: {status['last_ppl_date']}")
    
    if status['last_rdl_date'] and status['last_ppl_date']:
        if status['last_rdl_date'] == status['last_ppl_date']:
            print(f"  ✅ Данные синхронизированы")
        else:
            lag = (status['last_rdl_date'] - status['last_ppl_date']).days
            print(f"  ⚠️  Отставание: {lag} дней")
            print(f"  Необработанных записей: {status['unprocessed_count']}")
    elif status['last_ppl_date'] is None:
        print(f"  ⚠️  PPL таблица пустая")
    else:
        print(f"  ℹ️  RDL таблица пустая")
    
    print(f"\nРекомендация:")
    if status['needs_processing']:
        print(f"  ⚡ Запустите: python scripts/run_etl.py")
    else:
        print(f"  ✅ Обработка не требуется")

if __name__ == "__main__":
    main()
