#!/usr/bin/env python3
"""Initialize database for Yandex Webmaster."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.collector import WebmasterCollector

if __name__ == "__main__":
    print("Initializing database for Yandex Webmaster...")
    
    collector = WebmasterCollector()
    
    try:
        collector.initialize_database()
        print("✅ Database initialized successfully!")
        print(f"   Database: {collector.client.host_id}")
        print(f"   Schema: rdl")
        print(f"   Table: webmaster")
    except Exception as e:
        print(f"❌ Failed to initialize database: {e}")
        sys.exit(1)
