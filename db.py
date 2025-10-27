
import sqlite3

def init_db():
    conn = sqlite3.connect("finance.db")
    c = conn.cursor()

    # Таблица с лимитами по категориям
    c.execute("""
        CREATE TABLE IF NOT EXISTS category_limits (
            user_id INTEGER,
            category TEXT,
            limit_amount REAL,
            PRIMARY KEY (user_id, category)
        )
    """)

    # Таблица с расходами
    c.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            category TEXT,
            amount REAL,
            date TEXT
        )
    """)

    conn.commit()
    conn.close()

