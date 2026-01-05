#!/usr/bin/env python3
"""Run script for Yandex Webmaster collector."""
import sys
import os

# Добавляем src в путь
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core.collector import main

if __name__ == "__main__":
    main()
