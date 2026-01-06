#!/usr/bin/env python3
"""Тест ETL v2"""
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor_v2 import WebmasterETLProcessorV2

def test_small_batch():
    """Тест небольшого батча"""
    print("🧪 Тест ETL v2 (небольшой батч)")
    
    db = DatabaseManager()
    processor = WebmasterETLProcessorV2(db)
    
    # Проверяем статус
    status = processor.get_processing_status()
    print(f"\n📊 Статус:")
    print(f"   RDL записей всего: {status['unprocessed_count']}")
    
    if status['unprocessed_count'] > 0:
        print(f"\n⚡ Тестируем обработку первых 100 записей...")
        
        # Временно уменьшим batch_size для теста
        processor.batch_size = 100
        
        result = processor.process_incremental()
        print(f"\n✅ Результат теста:")
        print(f"   Обработано: {result['aggregated_inserted']} записей")
        print(f"   Ошибок: {result['errors']}")
        print(f"   Статус: {result['status']}")
        
        if result['aggregated_inserted'] > 0:
            print(f"\n🎉 УСПЕХ! ETL работает!")
            return True
    else:
        print("❌ Нет данных для теста")
    
    return False

if __name__ == "__main__":
    test_small_batch()
