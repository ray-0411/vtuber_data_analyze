import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt

TOP_PATH = Path(__file__).resolve().parent
VER2_PATH = TOP_PATH
DB_PATH = VER2_PATH / "data_2_3.db"

SKIP_TIME = "08:00"   # 若不要跳過，設成 None


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
    yt_vals = []
    tw_vals = []
    all_vals = []

    for t, yt, tw, all_v in rows:
        times.append(t)
        yt_vals.append(yt)
        tw_vals.append(tw)
        all_vals.append(all_v)

    # x 軸只顯示整點
    xticks_idx = [i for i, t in enumerate(times) if t.endswith(":00")]
    xticks_labels = [times[i] for i in xticks_idx]

    plt.figure(figsize=(12, 5))

    plt.plot(times, yt_vals, marker="o", markersize=2, linewidth=2.5, label="YouTube")
    plt.plot(times, tw_vals, marker="o", markersize=2, linewidth=2.5, label="Twitch")
    plt.plot(times, all_vals, marker="o", markersize=2, linewidth=2.5, label="All")


    plt.axhline(0, linestyle="--", linewidth=1, alpha=0.6)  # 0% 參考線

    plt.xticks(xticks_idx, xticks_labels, rotation=45)
    plt.xlabel("Time of Day")
    plt.ylabel("Avg Diff Percent (%)")
    plt.title("Time-based Performance (YT / TW / ALL)")

    plt.legend()
    plt.grid(True, linestyle=":", alpha=0.6)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
