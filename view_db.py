import sqlite3

db = sqlite3.connect('telemetry.db')
db.row_factory = sqlite3.Row

print("\n" + "="*60)
print("  RAW PACKETS")
print("="*60)
rows = db.execute('SELECT * FROM raw_packets ORDER BY id DESC LIMIT 5').fetchall()
if not rows:
    print("  No raw packets yet.")
for row in rows:
    print(f"\n  ID: {row['id']} | Received: {row['received_at']}")
    print(f"  {row['raw_text']}")

print("\n" + "="*60)
print("  DECODED PACKETS")
print("="*60)
rows = db.execute('SELECT * FROM decoded_packets ORDER BY id DESC LIMIT 5').fetchall()
if not rows:
    print("  No decoded packets yet.")
for row in rows:
    import json
    data = json.loads(row['payload_json'])
    print(f"\n  PKT #{row['packet_no']} | Uploaded: {'✓' if row['uploaded'] else '✗'} | {row['created_at']}")
    print(f"  T={data['TEMPERATURE']}°C | P={data['PRESSURE']}hPa | H={data['HUMIDITY']}%")
    print(f"  ALT={data['ALTITUDE']}m | ACCEL=({data['ACCEL_X']}, {data['ACCEL_Y']}, {data['ACCEL_Z']})")

print("\n" + "="*60)
print("  STATS")
print("="*60)
total_raw     = db.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]
total_decoded = db.execute('SELECT COUNT(*) FROM decoded_packets').fetchone()[0]
total_uploaded = db.execute('SELECT COUNT(*) FROM decoded_packets WHERE uploaded=1').fetchone()[0]
print(f"  Raw packets    : {total_raw}")
print(f"  Decoded packets: {total_decoded}")
print(f"  Uploaded       : {total_uploaded}")
print(f"  Pending upload : {total_decoded - total_uploaded}")
print("="*60)

db.close()