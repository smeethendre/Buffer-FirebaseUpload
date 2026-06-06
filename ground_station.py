import serial
import sqlite3
import threading
import queue
import time
import csv
import json
import requests
from datetime import datetime
from io import StringIO

# ── CONFIG ───────────────────────────────────────────────────────────────────
COM_PORT        = 'COM5'
BAUD_RATE       = 9600
DB_PATH         = 'telemetry.db'
HAB_ID          = 'HAB-MUM-01'
FIREBASE_URL    = 'https://leap-2df27-default-rtdb.firebaseio.com'
FIREBASE_SECRET = 'h7BHFpVU3okE1UhygPInLacvb7tp2Xb9NxR0YSMN'
# ─────────────────────────────────────────────────────────────────────────────

# CSV column order from STM32
CSV_HEADERS = [
    'seq',
    'bmp_t_01C',    # BMP temperature (°C × 10)
    'bmp_p_Pa',     # BMP pressure (Pa)
    'bmp_alt_m',    # BMP altitude (m × 10)
    'aht_t_01C',    # AHT temperature (°C × 10)
    'aht_h_01pc',   # AHT humidity (% × 10)
    'dsb_t_C',      # DS18B20 temperature (°C)
    'uv_V',         # UV sensor voltage
    'ax_mg',        # Accel X (mg)
    'ay_mg',        # Accel Y (mg)
    'az_mg',        # Accel Z (mg)
    'gps_lat',      # GPS latitude
    'gps_lon',      # GPS longitude
    'gps_fix',      # GPS fix status
    'gps_time',     # GPS time
    'gps_date',     # GPS date
]

# ── Thread-safe queues ────────────────────────────────────────────────────────
raw_queue    = queue.Queue()   # T1 → T2
upload_queue = queue.Queue()   # T2 → T3

# ── SQLite setup ──────────────────────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_packets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_text    TEXT,
            received_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS decoded_packets (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            packet_no    INTEGER,
            payload_json TEXT,
            uploaded     INTEGER DEFAULT 0,
            created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print("[DB] SQLite ready.")

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 1 — CAPTURE
# Read COM5 line by line → save raw to SQLite → push to raw_queue
# ═══════════════════════════════════════════════════════════════════════════════
def thread_capture():
    print(f"[T1-CAPTURE] Connecting to {COM_PORT} @ {BAUD_RATE} baud...")

    ser = None
    while ser is None:
        try:
            ser = serial.Serial(COM_PORT, BAUD_RATE, timeout=1)
            print(f"[T1-CAPTURE] Connected!")
        except Exception as e:
            print(f"[T1-CAPTURE] Waiting for port... ({e})")
            time.sleep(2)

    db = sqlite3.connect(DB_PATH)

    while True:
        try:
            raw  = ser.readline()
            if not raw:
                continue
            line = raw.decode('utf-8', errors='ignore').strip()
            if not line:
                continue

            # Skip header line if STM32 sends it on boot
            if line.startswith('seq') or line.startswith('#'):
                print(f"  [SERIAL] (header) {line}")
                continue

            print(f"  [SERIAL] {line}")

            # Save raw to SQLite
            db.execute('INSERT INTO raw_packets (raw_text) VALUES (?)', (line,))
            db.commit()

            # Push to parser
            raw_queue.put(line)

        except Exception as e:
            print(f"[T1-CAPTURE] Error: {e}")
            time.sleep(0.1)

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 2 — PARSER
# Take CSV line from raw_queue → parse → save decoded to SQLite → push to upload_queue
# ═══════════════════════════════════════════════════════════════════════════════
def parse_csv(line):
    try:
        reader = csv.reader(StringIO(line))
        values = next(reader)

        if len(values) < len(CSV_HEADERS):
            print(f"[T2-PARSER] Incomplete CSV: {len(values)} fields, expected {len(CSV_HEADERS)}")
            return None

        row = dict(zip(CSV_HEADERS, values))

        # Map CSV fields → dashboard fields
        data = {
            'HAB_ID':          HAB_ID,
            'PACKET_NO':       int(row['seq']),
            'MISSION_TIME':    datetime.utcnow().strftime('%H:%M:%S'),
            'TIMESTAMP':       datetime.utcnow().isoformat(),

            # Temperature — use BMP as primary, AHT as backup
            'TEMPERATURE':     round(int(row['bmp_t_01C']) / 10.0, 2),

            # Pressure — convert Pa to hPa
            'PRESSURE':        round(int(row['bmp_p_Pa']) / 100.0, 2),

            # Humidity — divide by 10
            'HUMIDITY':        round(int(row['aht_h_01pc']) / 10.0, 2),

            # Altitude — divide by 10
            'ALTITUDE':        round(int(row['bmp_alt_m']) / 10.0, 2),

            # UV
            'UV_INDEX':        float(row['uv_V']),

            # Accelerometer (mg)
            'ACCEL_X':         float(row['ax_mg']),
            'ACCEL_Y':         float(row['ay_mg']),
            'ACCEL_Z':         float(row['az_mg']),

            # Gyro not in CSV — set to 0
            'GYRO_X':          0.0,
            'GYRO_Y':          0.0,
            'GYRO_Z':          0.0,

            # Magnetic field not in CSV — set to 0
            'MAGNETIC_FIELD':  0.0,

            # GPS
            'LATITUDE':        float(row['gps_lat']),
            'LONGITUDE':       float(row['gps_lon']),

            # Extras
            'BATTERY_PERCENT': 100.0,
            'CAMERA_STATUS':   'ON',
            'STATUS_FLAG':     'OK',
            'RSSI':            0,

            # Extra fields from CSV
            'DSB_TEMP':        float(row['dsb_t_C']),
            'AHT_TEMP':        round(int(row['aht_t_01C']) / 10.0, 2),
            'GPS_FIX':         int(row['gps_fix']),
            'GPS_TIME':        str(row['gps_time']),
            'GPS_DATE':        str(row['gps_date']),
        }

        return data

    except Exception as e:
        print(f"[T2-PARSER] Parse error: {e} | Line: {line}")
        return None

def thread_parser():
    print("[T2-PARSER] Ready.")
    db = sqlite3.connect(DB_PATH)

    while True:
        try:
            line = raw_queue.get(timeout=1)

            parsed = parse_csv(line)
            if parsed is None:
                raw_queue.task_done()
                continue

            payload_json = json.dumps(parsed)

            # Save decoded to SQLite
            db.execute(
                'INSERT INTO decoded_packets (packet_no, payload_json) VALUES (?, ?)',
                (parsed['PACKET_NO'], payload_json)
            )
            db.commit()

            print(f"[T2-PARSER] PKT #{parsed['PACKET_NO']} | "
                  f"T={parsed['TEMPERATURE']}°C | "
                  f"P={parsed['PRESSURE']}hPa | "
                  f"ALT={parsed['ALTITUDE']}m | "
                  f"H={parsed['HUMIDITY']}% | "
                  f"GPS=({parsed['LATITUDE']},{parsed['LONGITUDE']})")

            # Push to uploader
            upload_queue.put(parsed)
            raw_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[T2-PARSER] Error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 3 — UPLOADER
# Take parsed dict from upload_queue → POST to Firebase → mark uploaded in SQLite
# ═══════════════════════════════════════════════════════════════════════════════
def thread_uploader():
    print("[T3-UPLOADER] Ready.")
    db  = sqlite3.connect(DB_PATH)
    url = f"{FIREBASE_URL}/telemetry.json?auth={FIREBASE_SECRET}"

    while True:
        try:
            data = upload_queue.get(timeout=1)

            try:
                resp = requests.post(url, json=data, timeout=5)

                if resp.status_code == 200:
                    db.execute(
                        'UPDATE decoded_packets SET uploaded = 1 WHERE packet_no = ?',
                        (data['PACKET_NO'],)
                    )
                    db.commit()
                    print(f"[T3-UPLOADER] PKT #{data['PACKET_NO']} → Firebase ✓")
                else:
                    print(f"[T3-UPLOADER] Firebase error {resp.status_code}: {resp.text}")
                    upload_queue.put(data)
                    time.sleep(3)

            except requests.exceptions.RequestException as e:
                print(f"[T3-UPLOADER] Network error: {e}")
                upload_queue.put(data)
                time.sleep(3)

            upload_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[T3-UPLOADER] Error: {e}")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 55)
    print("   HAB GROUND STATION — LEAP-HABSAT-01")
    print("   Firebase : leap-2df27-default-rtdb")
    print(f"   COM Port : {COM_PORT}  |  Baud : {BAUD_RATE}")
    print("=" * 55)

    init_db()

    t1 = threading.Thread(target=thread_capture,  daemon=True, name='T1-Capture')
    t2 = threading.Thread(target=thread_parser,   daemon=True, name='T2-Parser')
    t3 = threading.Thread(target=thread_uploader, daemon=True, name='T3-Uploader')

    t1.start()
    t2.start()
    t3.start()

    print("\n[MAIN] All 3 threads running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(10)
            print(f"[MAIN] Queues → raw: {raw_queue.qsize()} | upload: {upload_queue.qsize()}")
    except KeyboardInterrupt:
        print("\n[MAIN] Shutting down.")