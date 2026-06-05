import serial
import sqlite3
import threading
import queue
import time
import re
import json
import requests
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────────────────────
COM_PORT         = 'COM5'
BAUD_RATE        = 9600
DB_PATH          = 'telemetry.db'
HAB_ID           = 'HAB-MUM-01'
FIREBASE_URL     = 'https://leap-2df27-default-rtdb.firebaseio.com'
FIREBASE_SECRET  = 'h7BHFpVU3okE1UhygPInLacvb7tp2Xb9NxR0YSMN'
# ─────────────────────────────────────────────────────────────────────────────

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
# Read COM port → save raw text to SQLite → push to raw_queue
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

    buffer = []
    db     = sqlite3.connect(DB_PATH)

    while True:
        try:
            raw  = ser.readline()
            if not raw:
                continue
            line = raw.decode('utf-8', errors='ignore').strip()
            if not line:
                continue

            print(f"  [SERIAL] {line}")

            if line.startswith('---'):
                if buffer:
                    raw_text = '\n'.join(buffer)

                    # Save raw to SQLite
                    db.execute('INSERT INTO raw_packets (raw_text) VALUES (?)', (raw_text,))
                    db.commit()

                    # Push to parser
                    raw_queue.put(raw_text)
                    buffer = []
            else:
                buffer.append(line)

        except Exception as e:
            print(f"[T1-CAPTURE] Error: {e}")
            time.sleep(0.1)

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 2 — PARSER
# Take raw text from raw_queue → parse → save to SQLite → push to upload_queue
# ═══════════════════════════════════════════════════════════════════════════════
packet_counter = 1
counter_lock   = threading.Lock()

def parse_packet(raw_text):
    global packet_counter

    with counter_lock:
        pkt_no = packet_counter
        packet_counter += 1

    data = {
        'HAB_ID':          HAB_ID,
        'PACKET_NO':       pkt_no,
        'MISSION_TIME':    datetime.utcnow().strftime('%H:%M:%S'),
        'TIMESTAMP':       datetime.utcnow().isoformat(),
        'TEMPERATURE':     0.0,
        'PRESSURE':        0.0,
        'HUMIDITY':        0.0,
        'ALTITUDE':        0.0,
        'ACCEL_X':         0.0,
        'ACCEL_Y':         0.0,
        'ACCEL_Z':         0.0,
        'GYRO_X':          0.0,
        'GYRO_Y':          0.0,
        'GYRO_Z':          0.0,
        'UV_INDEX':        0.0,
        'MAGNETIC_FIELD':  0.0,
        'LATITUDE':        0.0,
        'LONGITUDE':       0.0,
        'BATTERY_PERCENT': 100.0,
        'CAMERA_STATUS':   'ON',
        'STATUS_FLAG':     'OK',
        'RSSI':            0,
    }

    for line in raw_text.split('\n'):
        line = line.strip()

        # MPU: AX=30 AY=-20 AZ=1029
        if line.startswith('MPU'):
            m = re.search(r'AX=([-\d.]+)\s+AY=([-\d.]+)\s+AZ=([-\d.]+)', line)
            if m:
                data['ACCEL_X'] = float(m.group(1))
                data['ACCEL_Y'] = float(m.group(2))
                data['ACCEL_Z'] = float(m.group(3))

        # BME: T=28.03C P=100368Pa H=40.01% ALT=77m
        elif line.startswith('BME'):
            m = re.search(
                r'T=([-\d.]+)C\s+P=([\d.]+)Pa\s+H=([\d.]+)%\s+ALT=([\d.]+)m',
                line
            )
            if m:
                data['TEMPERATURE'] = float(m.group(1))
                data['PRESSURE']    = round(float(m.group(2)) / 100, 2)
                data['HUMIDITY']    = float(m.group(3))
                data['ALTITUDE']    = float(m.group(4))

        # AHT: T=27.01C H=52.62%
        elif line.startswith('AHT'):
            m = re.search(r'T=([-\d.]+)C\s+H=([\d.]+)%', line)
            if m:
                if data['TEMPERATURE'] == 0.0:
                    data['TEMPERATURE'] = float(m.group(1))
                if data['HUMIDITY'] == 0.0:
                    data['HUMIDITY'] = float(m.group(2))

    return data

def thread_parser():
    print("[T2-PARSER] Ready.")
    db = sqlite3.connect(DB_PATH)

    while True:
        try:
            raw_text = raw_queue.get(timeout=1)

            parsed       = parse_packet(raw_text)
            payload_json = json.dumps(parsed)

            # Save decoded to SQLite
            db.execute(
                'INSERT INTO decoded_packets (packet_no, payload_json) VALUES (?, ?)',
                (parsed['PACKET_NO'], payload_json)
            )
            db.commit()

            print(f"[T2-PARSER] PKT #{parsed['PACKET_NO']} | "
                  f"T={parsed['TEMPERATURE']}°C | "
                  f"ALT={parsed['ALTITUDE']}m | "
                  f"H={parsed['HUMIDITY']}%")

            # Push to uploader
            upload_queue.put(parsed)
            raw_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[T2-PARSER] Error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 3 — UPLOADER
# Take parsed dict from upload_queue → POST to Firebase REST → mark uploaded
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
                    upload_queue.put(data)  # retry
                    time.sleep(3)

            except requests.exceptions.RequestException as e:
                print(f"[T3-UPLOADER] Network error: {e}")
                upload_queue.put(data)  # retry
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