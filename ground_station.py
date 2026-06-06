import serial
import serial.tools.list_ports
import sqlite3
import threading
import queue
import time
import csv
import json
import requests
import logging
import os
import sys
from datetime import datetime
from io import StringIO

# ── CONFIG ───────────────────────────────────────────────────────────────────
COM_PORT        = 'AUTO'         # 'AUTO' to auto-detect STM32, or set 'COM5'
BAUD_RATE       = 115200
DB_PATH         = 'telemetry.db'
LOG_PATH        = 'ground_station.log'
HAB_ID          = 'HAB-MUM-01'
FIREBASE_URL    = 'https://leap-2df27-default-rtdb.firebaseio.com'
FIREBASE_SECRET = 'h7BHFpVU3okE1UhygPInLacvb7tp2Xb9NxR0YSMN'

SIMULATION_MODE = True

# STM32 USB identifiers for auto-detection
STM32_VID_PID = [
    (0x0483, 0x5740),   # STM32 Virtual COM Port
    (0x0483, 0x374B),   # ST-Link V2
    (0x0483, 0x3748),   # ST-Link V1
]
# ─────────────────────────────────────────────────────────────────────────────

# CSV column order from STM32 transmitter
CSV_HEADERS = [
    'seq',
    'bmp_t_01C',
    'bmp_p_Pa',
    'bmp_alt_m',
    'aht_t_01C',
    'aht_h_01pc',
    'dsb_t_C',
    'uv_V',
    'ax_mg',
    'ay_mg',
    'az_mg',
    'gps_lat',
    'gps_lon',
    'gps_fix',
    'gps_time',
    'gps_date',
]

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger('HAB')

# ── Thread-safe queues ────────────────────────────────────────────────────────
raw_queue    = queue.Queue()
upload_queue = queue.Queue()

# ── Mission stats ─────────────────────────────────────────────────────────────
stats = {
    'packets_received': 0,
    'packets_parsed':   0,
    'packets_uploaded': 0,
    'parse_errors':     0,
    'upload_errors':    0,
    'start_time':       datetime.utcnow().isoformat(),
}
stats_lock = threading.Lock()

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
    c.execute('''
        CREATE TABLE IF NOT EXISTS mission_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event      TEXT,
            detail     TEXT,
            logged_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    log.info("SQLite ready — %s", os.path.abspath(DB_PATH))

def log_event(event, detail=''):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute('INSERT INTO mission_log (event, detail) VALUES (?, ?)', (event, detail))
        conn.commit()
        conn.close()
    except:
        pass

# ── Auto COM port detection ───────────────────────────────────────────────────
def find_stm32_port():
    ports = serial.tools.list_ports.comports()
    for port in ports:
        for vid, pid in STM32_VID_PID:
            if port.vid == vid and port.pid == pid:
                log.info("STM32 auto-detected on %s (%s)", port.device, port.description)
                return port.device
    # Fallback — return first available port with STM in description
    for port in ports:
        if port.description and 'STM' in port.description.upper():
            log.info("STM32 found by name on %s (%s)", port.device, port.description)
            return port.device
    return None

def get_port():
    if COM_PORT != 'AUTO':
        return COM_PORT
    detected = find_stm32_port()
    if detected:
        return detected
    log.warning("STM32 not found via auto-detect, falling back to COM5")
    return 'COM5'

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 1 — CAPTURE
# Read COM port → save raw to SQLite → push to raw_queue
# Auto-reconnects on disconnect
# ═══════════════════════════════════════════════════════════════════════════════
def thread_capture():
    log.info("[T1-CAPTURE] Started")

    while True:
        port = get_port()
        log.info("[T1-CAPTURE] Connecting to %s @ %d baud...", port, BAUD_RATE)

        ser = None
        while ser is None:
            try:
                ser = serial.Serial(port, BAUD_RATE, timeout=1)
                log.info("[T1-CAPTURE] Connected to %s!", port)
                log_event('CAPTURE_CONNECTED', port)
            except Exception as e:
                log.warning("[T1-CAPTURE] Port not ready, retrying in 3s... (%s)", e)
                time.sleep(3)
                # Re-detect port in case it changed
                port = get_port()

        db = sqlite3.connect(DB_PATH)

        try:
            while True:
                raw = ser.readline()
                if not raw:
                    continue

                line = raw.decode('utf-8', errors='ignore').strip()
                if not line:
                    continue

                # Skip header or comment lines
                if line.startswith('seq') or line.startswith('#') or line.startswith('['):
                    log.info("[T1-CAPTURE] (skip) %s", line)
                    continue

                log.info("[SERIAL] %s", line)

                # Save raw to SQLite
                db.execute('INSERT INTO raw_packets (raw_text) VALUES (?)', (line,))
                db.commit()

                # Push to parser queue
                raw_queue.put(line)

                with stats_lock:
                    stats['packets_received'] += 1

        except Exception as e:
            log.error("[T1-CAPTURE] Connection lost: %s", e)
            log_event('CAPTURE_DISCONNECTED', str(e))
            try:
                ser.close()
            except:
                pass
            log.info("[T1-CAPTURE] Reconnecting in 3s...")
            time.sleep(3)

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 2 — PARSER
# Take CSV from raw_queue → parse → save decoded to SQLite → push to upload_queue
# ═══════════════════════════════════════════════════════════════════════════════
def parse_csv(line):
    try:
        reader = csv.reader(StringIO(line))
        values = next(reader)

        if len(values) < len(CSV_HEADERS):
            log.warning("[T2-PARSER] Incomplete CSV: %d fields, expected %d | %s",
                        len(values), len(CSV_HEADERS), line)
            return None

        row = dict(zip(CSV_HEADERS, values))

        data = {
            'HAB_ID':          HAB_ID,
            'PACKET_NO':       int(row['seq']),
            'MISSION_TIME':    datetime.utcnow().strftime('%H:%M:%S'),
            'TIMESTAMP':       datetime.utcnow().isoformat(),
            'TEMPERATURE':     round(int(row['bmp_t_01C']) / 10.0, 2),
            'PRESSURE':        round(int(row['bmp_p_Pa']) / 100.0, 2),
            'HUMIDITY':        round(int(row['aht_h_01pc']) / 10.0, 2),
            'ALTITUDE':        round(int(row['bmp_alt_m']) / 10.0, 2),
            'UV_INDEX':        float(row['uv_V']),
            'ACCEL_X':         float(row['ax_mg']),
            'ACCEL_Y':         float(row['ay_mg']),
            'ACCEL_Z':         float(row['az_mg']),
            'GYRO_X':          0.0,
            'GYRO_Y':          0.0,
            'GYRO_Z':          0.0,
            'MAGNETIC_FIELD':  0.0,
            'LATITUDE':        float(row['gps_lat']),
            'LONGITUDE':       float(row['gps_lon']),
            'BATTERY_PERCENT': 100.0,
            'CAMERA_STATUS':   'ON',
            'STATUS_FLAG':     'OK',
            'RSSI':            0,
            'DSB_TEMP':        float(row['dsb_t_C']),
            'AHT_TEMP':        round(int(row['aht_t_01C']) / 10.0, 2),
            'GPS_FIX':         int(row['gps_fix']),
            'GPS_TIME':        str(row['gps_time']),
            'GPS_DATE':        str(row['gps_date']),
        }

        return data

    except Exception as e:
        log.error("[T2-PARSER] Parse error: %s | Line: %s", e, line)
        return None

def thread_parser():
    log.info("[T2-PARSER] Started")
    db = sqlite3.connect(DB_PATH)

    while True:
        try:
            line = raw_queue.get(timeout=1)

            parsed = parse_csv(line)
            if parsed is None:
                with stats_lock:
                    stats['parse_errors'] += 1
                raw_queue.task_done()
                continue

            payload_json = json.dumps(parsed)

            db.execute(
                'INSERT INTO decoded_packets (packet_no, payload_json) VALUES (?, ?)',
                (parsed['PACKET_NO'], payload_json)
            )
            db.commit()

            log.info("[T2-PARSER] PKT #%d | T=%.1f°C | P=%.1fhPa | "
                     "ALT=%.1fm | H=%.1f%% | GPS=(%.6f, %.6f)",
                     parsed['PACKET_NO'],
                     parsed['TEMPERATURE'],
                     parsed['PRESSURE'],
                     parsed['ALTITUDE'],
                     parsed['HUMIDITY'],
                     parsed['LATITUDE'],
                     parsed['LONGITUDE'])

            upload_queue.put(parsed)

            with stats_lock:
                stats['packets_parsed'] += 1

            raw_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            log.error("[T2-PARSER] Error: %s", e)

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD 3 — UPLOADER
# Take parsed dict from upload_queue → POST to Firebase → mark uploaded
# Retries failed uploads automatically
# ═══════════════════════════════════════════════════════════════════════════════
def thread_uploader():
    log.info("[T3-UPLOADER] Started")
    db  = sqlite3.connect(DB_PATH)
    url = f"{FIREBASE_URL}/telemetry.json?auth={FIREBASE_SECRET}"

    while True:
        try:
            data = upload_queue.get(timeout=1)

            uploaded = False
            retries  = 0

            while not uploaded and retries < 5:
                try:
                    resp = requests.post(url, json=data, timeout=5)

                    if resp.status_code == 200:
                        db.execute(
                            'UPDATE decoded_packets SET uploaded = 1 WHERE packet_no = ?',
                            (data['PACKET_NO'],)
                        )
                        db.commit()
                        log.info("[T3-UPLOADER] PKT #%d → Firebase ✓", data['PACKET_NO'])

                        with stats_lock:
                            stats['packets_uploaded'] += 1

                        uploaded = True
                    else:
                        log.warning("[T3-UPLOADER] Firebase %d: %s (retry %d/5)",
                                    resp.status_code, resp.text, retries + 1)
                        retries += 1
                        time.sleep(2)

                except requests.exceptions.RequestException as e:
                    log.warning("[T3-UPLOADER] Network error (retry %d/5): %s", retries + 1, e)
                    retries += 1
                    time.sleep(2)

            if not uploaded:
                log.error("[T3-UPLOADER] PKT #%d failed after 5 retries — will retry later",
                          data['PACKET_NO'])
                upload_queue.put(data)

                with stats_lock:
                    stats['upload_errors'] += 1

            upload_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            log.error("[T3-UPLOADER] Error: %s", e)

# ── STATS THREAD — prints mission stats every 60s ────────────────────────────
def thread_stats():
    while True:
        time.sleep(60)
        with stats_lock:
            log.info(
                "[STATS] Received: %d | Parsed: %d | Uploaded: %d | "
                "Parse errors: %d | Upload errors: %d",
                stats['packets_received'],
                stats['packets_parsed'],
                stats['packets_uploaded'],
                stats['parse_errors'],
                stats['upload_errors'],
            )
# ═══════════════════════════════════════════════════════════════════════════════
# SIMULATOR
# Generates fake STM32 CSV packets
# Pushes directly into raw_queue
# ═══════════════════════════════════════════════════════════════════════════════

import random


def generate_sim_packet(seq, altitude):

    bmp_t_01C = int((25 - altitude / 1000 * 2) * 10)

    try:
        bmp_p_Pa = max(
            1000,
            int(101325 * ((1 - altitude / 44330) ** 5.255))
        )
    except:
        bmp_p_Pa = 1000

    bmp_alt_m = int(altitude * 10)

    aht_t_01C = bmp_t_01C + random.randint(-5, 5)
    aht_h_01pc = random.randint(450, 700)

    dsb_t_C = round((bmp_t_01C / 10) + random.uniform(-1, 1), 1)

    uv_V = round(random.uniform(0.5, 4.0), 2)

    ax_mg = random.randint(-100, 100)
    ay_mg = random.randint(-100, 100)
    az_mg = random.randint(950, 1050)

    gps_lat = round(19.076000 + altitude / 1000000, 6)
    gps_lon = round(72.877700 + altitude / 1000000, 6)

    gps_fix = 1

    gps_time = datetime.utcnow().strftime("%H%M%S")
    gps_date = datetime.utcnow().strftime("%d%m%y")

    return (
        f"{seq},"
        f"{bmp_t_01C},"
        f"{bmp_p_Pa},"
        f"{bmp_alt_m},"
        f"{aht_t_01C},"
        f"{aht_h_01pc},"
        f"{dsb_t_C},"
        f"{uv_V},"
        f"{ax_mg},"
        f"{ay_mg},"
        f"{az_mg},"
        f"{gps_lat},"
        f"{gps_lon},"
        f"{gps_fix},"
        f"{gps_time},"
        f"{gps_date}"
    )


def thread_simulator():

    log.info("[SIMULATOR] Started")

    seq = 1
    altitude = 0.0

    phase = "ASCENT"

    while True:

        if phase == "ASCENT":

            altitude += random.uniform(200, 400)

            if altitude >= 30000:
                altitude = 30000
                phase = "DESCENT"

                log.info(
                    "[SIMULATOR] BALLOON BURST @ %.0f m",
                    altitude
                )

        elif phase == "DESCENT":

            altitude -= random.uniform(250, 500)

            if altitude <= 0:

                altitude = 0

                log.info(
                    "[SIMULATOR] LANDED"
                )

                time.sleep(5)

                phase = "ASCENT"
                seq = 1

        packet = generate_sim_packet(
            seq,
            altitude
        )

        log.info(
            "[SIMULATOR] PKT #%d | ALT=%.0f m",
            seq,
            altitude
        )

        # EXACT SAME PIPELINE AS STM32

        db = sqlite3.connect(DB_PATH)

        db.execute(
            'INSERT INTO raw_packets (raw_text) VALUES (?)',
            (packet,)
        )

        db.commit()
        db.close()

        raw_queue.put(packet)

        with stats_lock:
            stats['packets_received'] += 1

        seq += 1

        time.sleep(1)
# ── MAIN ──────────────────────────────────────────────────────────────────────
# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':

    log.info("=" * 55)
    log.info("  HAB GROUND STATION — LEAP-HABSAT-01")

    if SIMULATION_MODE:
        log.info("  MODE     : SIMULATOR")
    else:
        log.info("  MODE     : LIVE STM32")

    log.info("  Firebase : leap-2df27-default-rtdb")
    log.info("  Baud     : %d", BAUD_RATE)
    log.info("  Log      : %s", os.path.abspath(LOG_PATH))
    log.info("=" * 55)

    init_db()
    log_event('GROUND_STATION_START', datetime.utcnow().isoformat())

    # Thread 1
    if SIMULATION_MODE:
        t1 = threading.Thread(
            target=thread_simulator,
            daemon=True,
            name='T1-Simulator'
        )
    else:
        t1 = threading.Thread(
            target=thread_capture,
            daemon=True,
            name='T1-Capture'
        )

    # Thread 2
    t2 = threading.Thread(
        target=thread_parser,
        daemon=True,
        name='T2-Parser'
    )

    # Thread 3
    t3 = threading.Thread(
        target=thread_uploader,
        daemon=True,
        name='T3-Uploader'
    )

    # Thread 4
    t4 = threading.Thread(
        target=thread_stats,
        daemon=True,
        name='T4-Stats'
    )

    t1.start()
    t2.start()
    t3.start()
    t4.start()

    log.info("All threads running.\n")

    try:
        while True:
            time.sleep(10)

    except KeyboardInterrupt:
        log.info("Shutting down...")
        log_event(
            'GROUND_STATION_STOP',
            datetime.utcnow().isoformat()
        )