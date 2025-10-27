
import sqlite3

def init_db():
    conn = sqlite3.connect("data.sqlite")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        category TEXT,
        amount REAL,
        date TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS category_limits (
        user_id INTEGER,
        category TEXT,
        limit REAL,
        PRIMARY KEY (user_id, category)
    )""")
    conn.commit()
    conn.close()
