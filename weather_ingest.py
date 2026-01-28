import requests
import psycopg2
import logging
from datetime import datetime

# ---- CONFIG ----
DB_NAME = "weather_db"
DB_USER = "postgres"
DB_PASS = "mypassword"
DB_HOST = "localhost"
DB_PORT = "5432"
logging.basicConfig(filename="weather_ingest.log",
                    level="INFO",
                    format="%(asctime)s %(levelname)s %(message)s")
MAX_RETRIES = 3

CITY = "Austin"
API_KEY = "Your key"
URL = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&units=imperial&appid={API_KEY}"

# ---- FETCH DATA ----
print("Starting weather ingestion")
for attempt in range(1, MAX_RETRIES + 1):
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        break
    except Exception as e:
        logging.error(f"API attempt {attempt} failed: {e}")
        if attempt == MAX_RETRIES:
            raise

if "main" not in data:
    raise ValueError("Invalid API response: missing 'main")
temp = data["main"]["temp"]
humidity = data["main"]["humidity"]
if temp is None or humidity is None:
    raise ValueError("Missing temperature or humidity")
timestamp = datetime.now()

# ---- INSERT INTO POSTGRES ----
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO weather_data (city, temperature, humidity, collected_at)
        VALUES (%s, %s, %s, %s)
    """, (CITY, temp, humidity, timestamp))

    conn.commit()
    logging.info("✅ Data inserted successfully")
except Exception as e:
    logging.error(f"Database insert failed: {e}")
    if conn:
        conn.rollback()
    raise
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()

print("✅ Data inserted successfully!")
