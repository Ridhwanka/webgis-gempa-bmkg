from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import json
from dotenv import load_dotenv
import os

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

app = FastAPI(title="WebGIS Gempa BMKG API")

# CORS — supaya frontend Leaflet bisa akses API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

import subprocess, sys

@app.get("/api/fetch-bmkg")
def trigger_fetch():
    try:
        subprocess.Popen([sys.executable, "fetch_bmkg.py"])
        return {"status": "ok", "message": "Fetch BMKG triggered"}
    except Exception as e:
        return {"status": "error", "message": str(e)}