# HAB Ground Station — LEAP-HABSAT-01

## Architecture
HAB Balloon
↓ LoRa RF
Receiver STM32
↓ USB (COM5, 9600 baud)
Laptop (ground_station.py)
├── Thread 1 → Read COM5 → SQLite (raw_packets)
├── Thread 2 → Parse SQLite → SQLite (decoded_packets)
└── Thread 3 → Upload Firebase
↓
Firebase Realtime DB
↓
Dashboard (localhost:3000)

## Files
| File | Purpose |
|---|---|
| `ground_station.py` | Main script |
| `view_db.py` | View SQLite data |
| `telemetry.db` | Auto-created database |

## Install
```bash
pip install pyserial requests
```

## Run
```bash
python ground_station.py
```

## View Database
```bash
python view_db.py
```

## Packet Format from STM32
