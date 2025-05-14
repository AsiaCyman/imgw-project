import requests
import psycopg2
import os
from datetime import datetime

# Dane do bazy z Railway – najlepiej trzymać je jako zmienne środowiskowe
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", 5432)

# URL z danymi IMGW
url = "https://danepubliczne.imgw.pl/api/data/synop"

def fetch_data():
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_to_db(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )
    cur = conn.cursor()

    # Tworzymy prostą tabelę jeśli nie istnieje
    cur.execute("""
        CREATE TABLE IF NOT EXISTS imgw_data (
            id SERIAL PRIMARY KEY,
            stacja TEXT,
            data_pomiaru TIMESTAMP,
            temperatura NUMERIC,
            cisnienie NUMERIC
        )
    """)

    for item in data:
        cur.execute("""
            INSERT INTO imgw_data (stacja, data_pomiaru, temperatura, cisnienie)
            VALUES (%s, %s, %s, %s)
        """, (
            item.get("stacja"),
            datetime.strptime(item.get("data_pomiaru"), "%Y-%m-%d %H:%M:%S"),
            float(item.get("temperatura")) if item.get("temperatura") else None,
            float(item.get("cisnienie")) if item.get("cisnienie") else None,
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Dane zapisane do bazy danych!")

if __name__ == "__main__":
    data = fetch_data()
    save_to_db(data)
