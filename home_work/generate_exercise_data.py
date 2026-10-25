"""
generate_exercise_data.py

Creates the (deliberately messy) CSV files used by the Airflow practice exercises.
The data is deterministic: everyone who runs it gets exactly the same files.

Usage (from the project root, next to docker-compose.yaml):
    python3 home_work/generate_exercise_data.py   # writes into ./source_data
    python3 home_work/generate_exercise_data.py /some/dir   # another folder

Files created:
    support_tickets.csv           Exercise 1
    sensors_tel_aviv.csv          Exercise 2
    sensors_haifa.csv             Exercise 2
    sensors_jerusalem.csv         Exercise 2
    shipments.csv                 Exercise 3
"""
import csv
import math
import os
import random
import sys
from datetime import datetime, timedelta

OUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "source_data"
os.makedirs(OUT_DIR, exist_ok=True)


def write_csv(name, header, rows):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {path}  ({len(rows)} rows)")


def noisy(rnd, value):
    """Real-world text noise: random case and stray spaces."""
    r = rnd.random()
    if r < 0.60:
        return value
    if r < 0.75:
        return value.lower()
    if r < 0.90:
        return value.upper()
    return " " + value + " "


# --------------------------------------------------------------------------
# Exercise 1: customer support tickets
# --------------------------------------------------------------------------
def make_tickets():
    rnd = random.Random(11)
    base = datetime(2026, 9, 1)
    rows = []
    for i in range(120):
        created = base + timedelta(minutes=rnd.randint(0, 30 * 24 * 60))
        category = noisy(rnd, rnd.choice(["Billing", "Technical", "Account", "Shipping"]))
        priority = "" if rnd.random() < 0.10 else noisy(rnd, rnd.choice(["low", "medium", "high", "urgent"]))
        status = noisy(rnd, rnd.choice(["open", "closed", "closed", "pending"]))
        if status.strip().lower() == "closed" and rnd.random() > 0.05:
            hours = round(rnd.uniform(0.5, 72), 1)
        else:
            hours = ""
        rows.append([f"T-{1000 + i}", created.strftime("%Y-%m-%d %H:%M"),
                     f"C{rnd.randint(100, 160)}", category, priority, status, hours])
    # a bug in the ticketing system sends 3 tickets twice
    for idx in (5, 17, 40):
        rows.append(list(rows[idx]))
    write_csv("support_tickets.csv",
              ["ticket_id", "created_at", "customer_id", "category", "priority", "status", "resolution_hours"],
              rows)


# --------------------------------------------------------------------------
# Exercise 2: weather sensors, one file per city
# --------------------------------------------------------------------------
def make_sensors():
    cities = {
        # city: (base temperature, missing-value probability per cell)
        "tel_aviv": (27.0, 0.03),
        "haifa": (25.0, 0.06),
        "jerusalem": (22.0, 0.22),   # a flaky sensor
    }
    start = datetime(2026, 9, 1)
    for n, (city, (base_temp, p_missing)) in enumerate(cities.items()):
        rnd = random.Random(100 + n)
        rows = []
        for h in range(72):  # 3 days, hourly
            ts = start + timedelta(hours=h)
            temp = round(base_temp + 5 * math.sin((h % 24 - 9) / 24 * 2 * math.pi) + rnd.uniform(-1, 1), 1)
            hum = round(rnd.uniform(40, 80), 1)
            temp_cell, hum_cell = temp, hum
            if rnd.random() < p_missing:
                # the device reports either nothing or the sentinel value -999
                temp_cell = "" if rnd.random() < 0.5 else -999
            if rnd.random() < p_missing:
                hum_cell = "" if rnd.random() < 0.5 else -999
            rows.append([ts.strftime("%Y-%m-%d %H:%M:%S"), temp_cell, hum_cell])
        write_csv(f"sensors_{city}.csv", ["reading_time", "temperature_c", "humidity_pct"], rows)


# --------------------------------------------------------------------------
# Exercise 3: shipments
# --------------------------------------------------------------------------
def make_shipments():
    rnd = random.Random(23)
    base = datetime(2026, 8, 1)
    rows = []
    for i in range(150):
        shipped = base + timedelta(days=rnd.randint(0, 40))
        delivered = shipped + timedelta(days=rnd.randint(1, 9)) if rnd.random() > 0.12 else None
        carrier = noisy(rnd, rnd.choice(["DHL", "FedEx", "Israel Post", "UPS"]))
        city = rnd.choice(["Tel Aviv", "Haifa", "Eilat", "Beer Sheva", "Netanya"])
        weight = round(rnd.uniform(0.2, 25), 1)
        rows.append([f"S-{20000 + i}", shipped.strftime("%Y-%m-%d"),
                     delivered.strftime("%Y-%m-%d") if delivered else "",
                     carrier, city, weight])
    # real-world data problems
    for idx in (7, 63, 101):                       # missing shipment id
        rows[idx][0] = ""
    for idx in (14, 29, 55, 80, 120):              # weight is text
        rows[idx][5] = "N/A"
    for idx in (33, 90):                           # negative weight
        rows[idx][5] = -1.5
    for idx in (45, 130):                          # delivered before it was shipped
        shipped = datetime.strptime(rows[idx][1], "%Y-%m-%d")
        rows[idx][2] = (shipped - timedelta(days=2)).strftime("%Y-%m-%d")
    write_csv("shipments.csv",
              ["shipment_id", "shipped_date", "delivered_date", "carrier", "destination_city", "weight_kg"],
              rows)


if __name__ == "__main__":
    make_tickets()
    make_sensors()
    make_shipments()
    print("Done.")
