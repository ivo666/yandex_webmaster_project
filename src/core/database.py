"""Database manager for SQLAlchemy sessions."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(self):
        # Используем те же настройки, что и в webmaster_loader.py
        db_params = {
            'dbname': os.getenv('DB_NAME', 'yandex_webmaster'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        database_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        
        self.engine = create_engine(database_url, pool_pre_ping=True)
        self.session_factory = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
        self.Session = scoped_session(self.session_factory)
    
    @contextmanager
    def get_session(self):
        """Get a database session with context manager."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def close(self):
        """Close all connections."""
        self.Session.remove()
        if self.engine:
            self.engine.dispose()
