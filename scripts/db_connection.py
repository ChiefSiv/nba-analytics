import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        host=os.getenv("NBA_DB_HOST", "localhost"),
        port=os.getenv("NBA_DB_PORT", "5432"),
        dbname=os.getenv("NBA_DB_NAME", "NNBAAnalytics"),  # change if your DB name differs
        user=os.getenv("NBA_DB_USER", "postgres"),          # your Postgres user
        password=os.getenv("NBA_DB_PASSWORD", "ChiefSiv8587!"),
    
    )
