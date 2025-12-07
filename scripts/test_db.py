import psycopg2
from db_connection import get_connection

def test_connection():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        print("Database connection successful, test query returned:", result)
        cursor.close()
        conn.close()
    except Exception as e:
        print("Database connection failed:", e)

if __name__ == "__main__":
    test_connection()
