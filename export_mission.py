import sqlite3
import json
import csv

db = sqlite3.connect("telemetry.db")
db.row_factory = sqlite3.Row

rows = db.execute(
    """
    SELECT payload_json
    FROM decoded_packets
    ORDER BY packet_no
    """
).fetchall()

with open("mission_export.csv", "w", newline="") as f:

    writer = None

    for row in rows:

        data = json.loads(row["payload_json"])

        if writer is None:
            writer = csv.DictWriter(
                f,
                fieldnames=data.keys()
            )
            writer.writeheader()

        writer.writerow(data)

db.close()

print("Mission exported to mission_export.csv")