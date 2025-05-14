import requests
import psycopg2
import os

# Konfiguracja połączenia z Railway (wstaw swoje dane)
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_PORT = os.environ.get("DB_PORT")

# Pobranie danych z IMGW
response = requests.get("https://danepubliczne.imgw.pl/api/data/synop")
data = response.json()

# Połączenie z bazą
conn = psycopg2.connect(
    host=DB_HOST,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)

cur = conn.cursor()

# Utworzenie tabeli (jeśli nie istnieje)
cur.execute("""
    CREATE TABLE IF NOT EXISTS pogoda (
        id SERIAL PRIMARY KEY,
        stacja TEXT,
        data_pomiaru DATE,
        godzina_pomiaru INT,
        temperatura NUMERIC,
        suma_opadu NUMERIC,
        cisnienie NUMERIC
    )
""")

# Wstawianie danych
for rekord in data:
    cur.execute("""
        INSERT INTO pogoda (stacja, data_pomiaru, godzina_pomiaru, temperatura, suma_opadu, cisnienie)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        rekord.get('stacja'),
        rekord.get('data_pomiaru'),
        int(rekord.get('godzina_pomiaru')),
        float(rekord.get('temperatura')),
        float(rekord.get('suma_opadu') or 0),
        float(rekord.get('cisnienie') or 0)
    ))

conn.commit()
cur.close()
conn.close()
print("Dane zostały zapisane do bazy.")
