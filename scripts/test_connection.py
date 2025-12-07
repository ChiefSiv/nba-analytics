from db_connection import get_connection

try:
    conn = get_connection()
    print("Connected!", conn)
    conn.close()
except Exception as e:
    print("Connection failed:", e)