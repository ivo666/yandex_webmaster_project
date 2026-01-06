-- Таблица для отслеживания выполнения ETL процессов
CREATE TABLE IF NOT EXISTS ppl.etl_metadata (
    id SERIAL PRIMARY KEY,
    last_processed_date DATE,
    process_type VARCHAR(50),
    rows_processed INTEGER,
    duration_seconds INTEGER,
    status VARCHAR(20) DEFAULT 'success',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставляем начальную запись
INSERT INTO ppl.etl_metadata (last_processed_date, process_type, rows_processed)
VALUES (NULL, 'initial', 0)
ON CONFLICT (id) DO NOTHING;

-- Создаем индекс для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_etl_metadata_date ON ppl.etl_metadata(last_processed_date);
CREATE INDEX IF NOT EXISTS idx_etl_metadata_created ON ppl.etl_metadata(created_at);
