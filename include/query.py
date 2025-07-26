import sqlite3
import os
from store_news import get_db_path

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_NAME = 'news_feed.db'

db_path = get_db_path(DATA_DIR, DB_NAME)
print(db_path)

try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        #truncate = cursor.execute("""
        #               DELETE FROM articles
        #               """)
        #conn.commit()
        
        query = cursor.execute("""
                       SELECT * FROM articles
                       """)
        
        print(query.fetchall())
        conn.close()
except sqlite3.Error as e:
        print(e)