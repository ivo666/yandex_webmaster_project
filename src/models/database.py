from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from typing import Generator
from sqlalchemy import PrimaryKeyConstraint

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import settings

Base = declarative_base()

class WebmasterData(Base):
    """Модель для данных Яндекс.Вебмастер - соответствует таблице rdl.webm_api"""
    __tablename__ = 'webm_api'  # Имя таблицы в БД - ИЗМЕНЕНО!
    __table_args__ = (
        PrimaryKeyConstraint('date', 'page_path', 'query', 'device'),
        {'schema': 'rdl'}  # Схема в БД
    )

    date = Column(Date, nullable=False)
    page_path = Column(Text, nullable=False)
    query = Column(Text, nullable=False)
    demand = Column(Integer, nullable=False)
    impressions = Column(Integer, nullable=False)
    clicks = Column(Integer, nullable=False)
    position = Column(Float, nullable=False)
    device = Column(String(20), nullable=False)

    def __repr__(self):
        return f"<WebmasterData(date={self.date}, query={self.query[:30]}..., device={self.device})>"

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

def create_tables():
    """Создает таблицы в БД (если нужно)"""
    Base.metadata.create_all(bind=engine)
