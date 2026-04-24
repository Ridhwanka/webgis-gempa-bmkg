import requests
import psycopg2
from datetime import datetime
import json
from dotenv import load_dotenv
import os
import subprocess, sys
import requests as req_http  # tambahkan di bagian import atas

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=5432
    )

# Koneksi ke PostgreSQL
conn = get_conn()
cur = conn.cursor()

# Buat tabel kalau belum ada
cur.execute("""
    CREATE TABLE IF NOT EXISTS gempa (
        id SERIAL PRIMARY KEY,
        tanggal DATE,
        jam TIME,
        datetime TIMESTAMP,
        magnitude FLOAT,
        kedalaman VARCHAR(50),
        wilayah TEXT,
        potensi TEXT,
        dirasakan TEXT,
        geom GEOMETRY(Point, 4326),
        fetched_at TIMESTAMP DEFAULT NOW()
    );
""")
conn.commit()

# Fetch dari BMKG
url = "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"
response = requests.get(url)
data = response.json()

gempa_list = data["Infogempa"]["gempa"]

for g in gempa_list:
    # --- Fix Lintang ---
    lintang_str = g["Lintang"].replace("°", "").strip()
    if "LU" in lintang_str:
        lat = float(lintang_str.replace(" LU", "").strip())
    elif "LS" in lintang_str:
        lat = -float(lintang_str.replace(" LS", "").strip())
    else:
        lat = float(lintang_str)

    # --- Fix Bujur ---
    lon = float(g["Bujur"].replace(" BT", "").replace("°", "").strip())
    

    # --- Fix Jam: hapus suffix WIB / WITA / WIT ---
    jam_str = g["Jam"].split(" ")[0]  # ambil "10:17:08" saja, buang "WIB"

    cur.execute("""
        INSERT INTO gempa 
            (tanggal, jam, magnitude, kedalaman, wilayah, potensi, dirasakan, geom)
        VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        ON CONFLICT ON CONSTRAINT gempa_unique DO NOTHING
    """, (
        g["Tanggal"], jam_str,
        float(g["Magnitude"]),
        g["Kedalaman"], g["Wilayah"],
        g.get("Potensi", ""), g.get("Dirasakan", ""),
        lon, lat
))

conn.commit()
cur.close()
conn.close()
print(f"✅ Berhasil simpan {len(gempa_list)} data gempa ke database!")