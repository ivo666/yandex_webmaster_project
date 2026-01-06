from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import settings

# Базовый класс только для ppl моделей
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

# Создаем движок базы данных
engine = create_engine(
    settings.db.connection_string,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True
)

# Создаем фабрику сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db() -> Generator:
    """Контекстный менеджер для работы с БД"""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

# Импортируем ppl модели
from models.ppl.models import WebmasterAggregated, WebmasterPositions, WebmasterClicks

# Функция для создания всех таблиц
def create_all_tables():
    """Create all tables for both rdl and ppl layers."""
    Base.metadata.create_all(bind=engine)
