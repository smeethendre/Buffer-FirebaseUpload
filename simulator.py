import random
import time
from datetime import datetime

def generate_packet(seq, altitude):
    bmp_t_01C = int((25 - altitude / 1000 * 2) * 10)

    bmp_p_Pa = max(
        1000,
        int(101325 * (1 - altitude / 44330) ** 5.255)
    )

    bmp_alt_m = int(altitude * 10)

    aht_t_01C = bmp_t_01C + random.randint(-5, 5)
    aht_h_01pc = random.randint(400, 700)

    dsb_t_C = round((bmp_t_01C / 10) + random.uniform(-1, 1), 1)

    uv_V = round(random.uniform(0.5, 4.5), 2)

    ax_mg = random.randint(-100, 100)
    ay_mg = random.randint(-100, 100)
    az_mg = random.randint(950, 1050)

    gps_lat = round(19.0760 + altitude / 1000000, 6)
    gps_lon = round(72.8777 + altitude / 1000000, 6)

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
    print("[SIMULATOR] Started")

    seq = 1
    altitude = 0
    ascending = True

    while True:

        if ascending:
            altitude += random.uniform(100, 250)

            if altitude >= 30000:
                print("[SIMULATOR] BALLOON BURST")
                ascending = False

        else:
            altitude -= random.uniform(150, 350)

            if altitude <= 0:
                altitude = 0
                print("[SIMULATOR] LANDED")

        packet = generate_packet(seq, altitude)

        print(
            f"[SIM] PKT #{seq} | ALT={altitude:.0f}m"
        )

        raw_queue.put(packet)

        seq += 1

        time.sleep(1)