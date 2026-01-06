#!/bin/bash
# Ежедневный скрипт запуска ETL
cd /home/pf-server/yandex_webmaster_project

LOG_FILE="logs/etl_daily_$(date +%Y%m%d).log"

echo "========================================" >> $LOG_FILE
echo "🚀 Запуск ETL процесса: $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE

# Запускаем ETL
python -c "
import sys
sys.path.insert(0, '.')
from src.core.database import DatabaseManager
from src.etl.webmaster_etl_processor_v2 import WebmasterETLProcessorV2
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.FileHandler('$LOG_FILE', mode='a'), logging.StreamHandler()]
)

db = DatabaseManager()
processor = WebmasterETLProcessorV2()
result = processor.process_incremental()

print(f'📊 Результат: {result[\"aggregated_inserted\"]} новых записей')
" >> $LOG_FILE 2>&1

echo "========================================" >> $LOG_FILE
echo "🏁 Завершение ETL: $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE
