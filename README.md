# 🚀 HAB Ground Station — LEAP-HABSAT-01

Ground station software for the **LEAP-HABSAT-01 High Altitude Balloon mission**. The system receives telemetry packets from an STM32-based receiver over serial communication, performs CSV packet decoding, stores data locally using SQLite, and reliably uploads telemetry to Firebase for live dashboard visualization.

Designed with an **offline-first, multi-threaded architecture** to prevent packet loss during flight operations.

---

## System Architecture

```text
High Altitude Balloon
        │
        │ LoRa RF (433 MHz)
        ▼
STM32 Receiver
        │
        │ USB Serial (115200 baud)
        ▼
Ground Station Software (Python)
        │
        ├── Thread 1 — Capture
        │     Read Serial Data
        │     ↓
        │     Store Raw Packets
        │     ↓
        │     SQLite (raw_packets)
        │
        ├── Thread 2 — Parser
        │     Decode CSV Telemetry
        │     ↓
        │     SQLite (decoded_packets)
        │
        ├── Thread 3 — Uploader
        │     Upload to Firebase
        │     ↓
        │     Firebase Realtime Database
        │     ↓
        │     Next.js Dashboard
        │
        └── Thread 4 — Stats
              Mission telemetry counters
              Logged every 60s
```

---

## Features

### Automatic STM32 Detection

- Detects receiver COM port automatically via USB VID/PID matching.
- Eliminates manual COM port configuration.
- Recovers automatically after device reconnection or USB replug.

---

### Multi-Threaded Processing

Four independent worker threads ensure smooth, non-blocking telemetry flow:

**Thread 1 — Serial Capture**
- Reads CSV telemetry packets from STM32.
- Saves every raw line to SQLite before any processing.
- Pushes raw text to a thread-safe queue for the parser.
- Auto-reconnects within 3 seconds if the USB connection drops mid-flight.

**Thread 2 — Packet Parser**
- Consumes raw CSV lines from the capture queue.
- Parses fixed-format CSV into structured telemetry fields.
- Converts raw sensor units (e.g. Pa → hPa, °C×10 → °C).
- Stores decoded JSON payloads in SQLite.

**Thread 3 — Firebase Uploader**
- Consumes decoded packets from the parser queue.
- Pushes telemetry to Firebase Realtime Database via REST API.
- Retries up to 5 times on network or server failure.
- Marks packets as uploaded only after confirmed success.

**Thread 4 — Mission Stats**
- Logs received/parsed/uploaded counts every 60 seconds.
- Tracks parse errors and upload failures for post-mission review.

---

### Local SQLite Persistence

Three tables are maintained for complete data integrity:

**`raw_packets`**
- Every raw CSV line exactly as received
- Reception timestamp
- Full packet backup independent of parsing success

**`decoded_packets`**
- Packet number
- Temperature (BMP280, AHT, DS18B20)
- Pressure, humidity, altitude
- UV sensor reading
- Accelerometer X/Y/Z
- GPS latitude, longitude, fix status, time, date
- Upload status flag

**`mission_log`**
- Connection/disconnection events
- Mission start/stop timestamps
- System-level events for post-flight debugging

This architecture enables:
- Post-flight data analysis
- Full data recovery even if Firebase upload fails entirely
- Fully offline operation during connectivity gaps

---

### Offline-First Design

```text
STM32
   ↓
Serial Input
   ↓
SQLite Buffer (always succeeds)
   ↓
Internet Available?
      │
      ├── No → Packet stays queued, retried automatically
      │
      └── Yes → Upload to Firebase → Dashboard updates live
```

No telemetry is ever lost due to a temporary network outage — every packet is durably written to SQLite before any network call is attempted.

---

## Repository Structure

```text
ground-station/
│
├── ground_station.py      # Main application — all 4 threads
├── view_db.py              # CLI tool to inspect SQLite database
├── telemetry.db             # Auto-generated on first run
├── ground_station.log       # Rolling log file
└── README.md
```

---

## Installation

### Clone Repository

```bash
git clone <repository-url>
cd ground-station
```

### Install Dependencies

```bash
pip install pyserial requests
```

---

## Configuration

Open `ground_station.py` and set these values at the top:

```python
COM_PORT        = 'AUTO'      # 'AUTO' to auto-detect STM32, or set e.g. 'COM5'
BAUD_RATE       = 115200
HAB_ID          = 'HAB-MUM-01'
FIREBASE_URL    = 'https://leap-2df27-default-rtdb.firebaseio.com'
FIREBASE_SECRET = '<your-firebase-database-secret>'
```

---

## Running the Ground Station

```bash
python ground_station.py
```

The application automatically:
- Detects the STM32 receiver on USB.
- Creates the SQLite database if it doesn't already exist.
- Starts all 4 worker threads.
- Begins telemetry acquisition the moment the STM32 is connected.
- Buffers every packet locally before attempting upload.
- Uploads telemetry to Firebase continuously, retrying on failure.

Expected console output:

```
=======================================================
   HAB GROUND STATION — LEAP-HABSAT-01
   Firebase : leap-2df27-default-rtdb
   Baud     : 115200
=======================================================
[DB] SQLite ready.
[T1-CAPTURE] Connected to COM5!
  [SERIAL] 1,1376,236,30344,2667,5773,25.93,0.001,18,-16,1017,0,0,0,0,0
[T2-PARSER] PKT #1 | T=137.6°C | P=2.4hPa | ALT=3034.4m | H=576.6%
[T3-UPLOADER] PKT #1 → Firebase ✓
```

---

## Viewing Stored Data

```bash
python view_db.py
```

Displays:
- Last 5 raw packets with timestamps
- Last 5 decoded packets with full sensor breakdown
- Mission statistics: total received, parsed, uploaded, and pending counts

---

## Telemetry Flow

```text
Balloon
    ↓
LoRa Radio
    ↓
STM32 Receiver
    ↓
USB Serial (CSV)
    ↓
ground_station.py
    ↓
SQLite Buffer (raw_packets)
    ↓
Packet Decoder
    ↓
SQLite (decoded_packets)
    ↓
Firebase Realtime Database
    ↓
Live Next.js Dashboard
```

---

## CSV Telemetry Format

```
seq,bmp_t_01C,bmp_p_Pa,bmp_alt_m,aht_t_01C,aht_h_01pc,dsb_t_C,uv_V,ax_mg,ay_mg,az_mg,gps_lat,gps_lon,gps_fix,gps_time,gps_date
```

| Field | Description | Unit |
|---|---|---|
| seq | Packet sequence number | — |
| bmp_t_01C | BMP280 temperature | °C × 10 |
| bmp_p_Pa | BMP280 pressure | Pa |
| bmp_alt_m | BMP280 altitude | m × 10 |
| aht_t_01C | AHT sensor temperature | °C × 10 |
| aht_h_01pc | AHT sensor humidity | % × 10 |
| dsb_t_C | DS18B20 temperature | °C |
| uv_V | UV sensor voltage | V |
| ax_mg, ay_mg, az_mg | Accelerometer X/Y/Z | mg |
| gps_lat, gps_lon | GPS coordinates | decimal degrees |
| gps_fix | GPS fix status | 0/1 |
| gps_time, gps_date | GPS timestamp | HHMMSS / YYYYMMDD |

---

## Technology Stack

**Language**
- Python 3.10+

**Communication**
- Serial UART (USB CDC)
- LoRa RF (433 MHz, via STM32 receiver)

**Database**
- SQLite (local persistence)

**Cloud**
- Firebase Realtime Database (REST API)

**Libraries**
- `pyserial` — serial port communication and auto-detection
- `sqlite3` — local database
- `requests` — Firebase REST uploads
- `threading` / `queue` — concurrent pipeline architecture
- `csv` — telemetry parsing
- `logging` — structured file + console logging

---

## Reliability Features

✅ Multi-threaded, non-blocking architecture
✅ Automatic STM32 COM port detection
✅ Local packet buffering — zero data loss on network outage
✅ Offline-first operation
✅ Automatic upload retry (5 attempts per packet)
✅ Auto-reconnect on USB disconnect (3s retry interval)
✅ Raw and decoded packet storage for full traceability
✅ Mission event logging (connect/disconnect/start/stop)
✅ Rolling stats every 60 seconds
✅ Post-flight analysis support via `view_db.py`

---

## Running as a Background Service

For unattended mission operation, configure the script to launch automatically on system boot using **Windows Task Scheduler**:

1. Open Task Scheduler → Create Task
2. Trigger: **At startup**
3. Action: Start `python.exe` with argument `ground_station.py`, working directory set to the repo folder
4. Settings: Enable **"Restart on failure"** with 1-minute retry interval, up to 999 attempts

This ensures the ground station resumes automatically if the laptop reboots or the script crashes mid-mission — no manual intervention required.

---

## Future Improvements

- CRC validation for packet integrity
- Packet acknowledgement/uplink mechanism
- CSV export of full mission dataset
- MQTT support as an alternative upload path
- Mission replay tooling for post-flight review
- Real-time packet-loss statistics dashboard

---

## Project Goal

To provide a **fault-tolerant ground segment software system** capable of reliably acquiring, buffering, decoding, and transmitting telemetry from the **LEAP-HABSAT-01 High Altitude Balloon mission**, ensuring uninterrupted data availability for real-time monitoring and post-flight analysis.
