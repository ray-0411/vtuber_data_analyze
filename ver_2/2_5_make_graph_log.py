import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import math

TOP_PATH = Path(__file__).resolve().parent
VER2_PATH = TOP_PATH
DB_PATH = VER2_PATH / "data_2_3.db"

SKIP_TIME = "08:00"   # 和 2_4 一致，若不要跳過設成 None


def safe_log_ratio(percent):
    """
    percent -> log(1 + percent / 100)
    防止 <= -100% 的非法情況
    """
    ratio = 1 + percent / 100.0
    if ratio <= 0:
        return 0.0
    return math.log(ratio)


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError("找不到 data_2_3.db")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        SELECT time, yt_avg, tw_avg, all_avg
        FROM yt_tw_time_avg
        ORDER BY time;
    """)
    rows = cur.fetchall()
    conn.close()

    times = []
    yt_log = []
    tw_log = []
    all_log = []

    for t, yt, tw, all_v in rows:

        times.append(t)
        yt_log.append(safe_log_ratio(yt))
        tw_log.append(safe_log_ratio(tw))
        all_log.append(safe_log_ratio(all_v))

    # x 軸只顯示整點
    xticks_idx = [i for i, t in enumerate(times) if t.endswith(":00")]
    xticks_labels = [times[i] for i in xticks_idx]

    plt.figure(figsize=(12, 5))

    plt.plot(times, yt_log, marker="o", markersize=3, linewidth=2, label="YouTube")
    plt.plot(times, tw_log, marker="o", markersize=3, linewidth=2, label="Twitch")
    plt.plot(times, all_log, marker="o", markersize=3, linewidth=2, label="All")

    plt.axhline(0, linestyle="--", linewidth=1, alpha=0.6)  # log(1)=0

    plt.xticks(xticks_idx, xticks_labels, rotation=45)
    plt.xlabel("Time of Day")
    plt.ylabel("Log Performance Ratio")
    plt.title("Time-based Performance (Log Ratio, YT / TW / ALL)")

    plt.legend()
    plt.grid(True, linestyle=":", alpha=0.6)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
