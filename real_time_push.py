# real_time_push.py
# Simple Python script that reads the last row of live_sensor.csv
# and pushes a small JSON object to a Realtime DB via REST.

import csv
import time
import requests


DATABASE_URL = "https://roadrisklive-default-rtdb.asia-southeast1.firebasedatabase.app"
NODE = "live_area"

def push(data):
    """Overwrite the node with the latest data (simple)."""
    url = f"{DATABASE_URL}/{NODE}.json"
    try:
        r = requests.put(url, json=data, timeout=5)
        r.raise_for_status()
    except Exception as e:
        print("Error pushing to Firebase:", e)

def read_latest(path):
    """Read CSV and return the last row as a dict (or None)."""
    try:
        with open(path, "r") as f:
            rows = list(csv.DictReader(f))
            return rows[-1] if rows else None
    except FileNotFoundError:
        return None
    except Exception as e:
        print("Error reading CSV:", e)
        return None

def compute_risk(acceleration, visibility=5):
    """
    Simple risk heuristic (explainable):
    - pothole detected if acc > 5
    - rough if acc > 3
    - visibility is 0-10 (lower -> worse)
    """
    pothole = acceleration > 5.0
    rough = acceleration > 3.0
    risk = 0
    if pothole: risk += 50
    if rough:   risk += 30
    if visibility < 4: risk += 20
    return int(risk), pothole, rough

def main_loop(csv_path="live_sensor.csv", interval=1):
    print("Starting real_time_push loop. Reading:", csv_path)
    while True:
        latest = read_latest(csv_path)
        if latest:
            try:
                lat = float(latest.get("lat", 0))
                lon = float(latest.get("lon", 0))
                acc = float(latest.get("acceleration", 0))
                ts = latest.get("timestamp", "")
            except Exception as e:
                print("Parse error for latest row:", e)
                time.sleep(interval)
                continue

            risk, pothole, rough = compute_risk(acc, visibility=5)

            payload = {
                "lat": lat,
                "lon": lon,
                "acc": acc,
                "pothole": pothole,
                "rough": rough,
                "visibility": 5,
                "risk_score": risk,
                "timestamp": ts
            }

            push(payload)
            print("Pushed:", payload)

        time.sleep(interval)

if __name__ == "__main__":
    main_loop()