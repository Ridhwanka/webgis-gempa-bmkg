from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
from dotenv import load_dotenv
import os
import subprocess, sys
import requests as req_http  # tambahkan di bagian import atas

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("host"),
        database=os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("Password")
    )

app = FastAPI(title="WebGIS Gempa BMKG API")

# CORS — supaya frontend Leaflet bisa akses API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Endpoint 1: Semua gempa sebagai GeoJSON ---
@app.get("/api/gempa")
def get_gempa(min_mag: float = 0.0, max_mag: float = 10.0):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            id, tanggal::text, jam::text, magnitude, 
            kedalaman, wilayah, potensi, dirasakan,
            ST_X(geom) as lon,
            ST_Y(geom) as lat
        FROM gempa
        WHERE magnitude BETWEEN %s AND %s
        ORDER BY tanggal DESC, jam DESC
    """, (min_mag, max_mag))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()

    features = []
    for row in rows:
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row[8], row[9]]  # lon, lat
            },
            "properties": {
                "id": row[0],
                "tanggal": row[1],
                "jam": row[2],
                "magnitude": row[3],
                "kedalaman": row[4],
                "wilayah": row[5],
                "potensi": row[6],
                "dirasakan": row[7]
            }
        })

    return {
        "type": "FeatureCollection",
        "count": len(features),
        "features": features
    }

# --- Endpoint 2: Statistik ringkas ---
@app.get("/api/stats")
def get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            MAX(magnitude) as max_mag,
            MIN(magnitude) as min_mag,
            ROUND(AVG(magnitude)::numeric, 2) as avg_mag
        FROM gempa
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()

    return {
        "total_gempa": row[0],
        "magnitude_max": row[1],
        "magnitude_min": row[2],
        "magnitude_rata_rata": row[3]
    }



@app.get("/api/fetch-bmkg")
def trigger_fetch():
    try:
        url = "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json"
        response = req_http.get(url, timeout=10)
        data = response.json()
        gempa_list = data["Infogempa"]["gempa"]

        conn = get_conn()
        cur = conn.cursor()

        count = 0
        for g in gempa_list:
            lintang_str = g["Lintang"].replace("°", "").strip()
            if "LU" in lintang_str:
                lat = float(lintang_str.replace(" LU", "").strip())
            elif "LS" in lintang_str:
                lat = -float(lintang_str.replace(" LS", "").strip())
            else:
                lat = float(lintang_str)

            lon = float(g["Bujur"].replace(" BT", "").replace("°", "").strip())
            jam_str = g["Jam"].split(" ")[0]

            cur.execute("""
                INSERT INTO gempa 
                    (tanggal, jam, magnitude, kedalaman, wilayah, potensi, dirasakan, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            """, (
                g["Tanggal"], jam_str,
                float(g["Magnitude"]),
                g["Kedalaman"], g["Wilayah"],
                g.get("Potensi", ""), g.get("Dirasakan", ""),
                lon, lat
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "ok", "fetched": count}

    except Exception as e:
        return {"status": "error", "message": str(e)}