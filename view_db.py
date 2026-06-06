import sqlite3
import json

db = sqlite3.connect('telemetry.db')
db.row_factory = sqlite3.Row

print("\n" + "=" * 70)
print("  RAW PACKETS (last 5)")
print("=" * 70)
rows = db.execute('SELECT * FROM raw_packets ORDER BY id DESC LIMIT 5').fetchall()
if not rows:
    print("  No raw packets yet.")
for row in rows:
    print(f"\n  ID: {row['id']} | Received: {row['received_at']}")
    print(f"  {row['raw_text']}")

print("\n" + "=" * 70)
print("  DECODED PACKETS (last 5)")
print("=" * 70)
rows = db.execute('SELECT * FROM decoded_packets ORDER BY id DESC LIMIT 5').fetchall()
if not rows:
    print("  No decoded packets yet.")
for row in rows:
    data = json.loads(row['payload_json'])
    uploaded = '✓ Uploaded' if row['uploaded'] else '✗ Pending'
    print(f"""
  PKT #{data['PACKET_NO']} | {uploaded} | {row['created_at']}
  ── Environment ──────────────────────────────
  BMP Temp     : {data['TEMPERATURE']} °C
  AHT Temp     : {data.get('AHT_TEMP', 0)} °C
  DSB Temp     : {data.get('DSB_TEMP', 0)} °C
  Pressure     : {data['PRESSURE']} hPa
  Humidity     : {data['HUMIDITY']} %
  Altitude     : {data['ALTITUDE']} m
  UV           : {data['UV_INDEX']} V
  ── Motion ───────────────────────────────────
  Accel X      : {data['ACCEL_X']} mg
  Accel Y      : {data['ACCEL_Y']} mg
  Accel Z      : {data['ACCEL_Z']} mg
  ── GPS ──────────────────────────────────────
  Latitude     : {data['LATITUDE']}
  Longitude    : {data['LONGITUDE']}
  GPS Fix      : {data.get('GPS_FIX', 0)}
  GPS Time     : {data.get('GPS_TIME', 0)}
  GPS Date     : {data.get('GPS_DATE', 0)}
  ── Mission ──────────────────────────────────
  HAB ID       : {data['HAB_ID']}
  Mission Time : {data['MISSION_TIME']}
  Timestamp    : {data['TIMESTAMP']}
  Status       : {data['STATUS_FLAG']}
  Battery      : {data['BATTERY_PERCENT']} %
    """)

print("=" * 70)
print("  STATS")
print("=" * 70)
total_raw      = db.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]
total_decoded  = db.execute('SELECT COUNT(*) FROM decoded_packets').fetchone()[0]
total_uploaded = db.execute('SELECT COUNT(*) FROM decoded_packets WHERE uploaded=1').fetchone()[0]
total_pending  = total_decoded - total_uploaded
print(f"""
  Raw packets     : {total_raw}
  Decoded packets : {total_decoded}
  Uploaded        : {total_uploaded}
  Pending upload  : {total_pending}
""")
print("=" * 70)

db.close()