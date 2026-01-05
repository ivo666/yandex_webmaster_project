"""Configuration management."""
import os
from pathlib import Path
from typing import Optional

# Для Pydantic 2.x используем pydantic-settings
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings."""
    
    # API settings
    yandex_oauth_token: str = Field(..., env="YANDEX_OAUTH_TOKEN")
    webmaster_user_id: Optional[str] = Field(None, env="WEBMASTER_USER_ID")
    webmaster_host_id: Optional[str] = Field(None, env="WEBMASTER_HOST_ID")
    
    # Database settings
    db_host: str = Field("localhost", env="DB_HOST")
    db_port: int = Field(5432, env="DB_PORT")
    db_name: str = Field("yandex_webmaster_db", env="DB_NAME")
    db_user: str = Field("postgres", env="DB_USER")
    db_password: str = Field(..., env="DB_PASSWORD")
    
    # Paths
    data_dir: Path = Field(Path("data"), env="DATA_DIR")
    log_dir: Path = Field(Path("logs"), env="LOG_DIR")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()