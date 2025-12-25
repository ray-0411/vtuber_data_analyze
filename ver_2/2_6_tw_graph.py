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
        SELECT time, tw_avg
        FROM yt_tw_time_avg
        ORDER BY time;
    """)
    rows = cur.fetchall()
    conn.close()

    times = []
    tw_vals = []

    for t, tw in rows:
        times.append(t)
        tw_vals.append(tw)

    # x 軸只顯示整點
    xticks_idx = [i for i, t in enumerate(times) if t.endswith(":00")]
    xticks_labels = [times[i] for i in xticks_idx]

    plt.figure(figsize=(12, 5))

    plt.plot(
        times,
        tw_vals,
        marker="o",
        markersize=3,
        linewidth=2.5,
        color="tab:purple",
        label="Twitch"
    )

    plt.axhline(0, linestyle="--", linewidth=1, alpha=0.6)

    plt.xticks(xticks_idx, xticks_labels, rotation=45)
    plt.xlabel("Time of Day")
    plt.ylabel("Avg Diff Percent (%)")
    plt.title("Twitch Time-based Performance (Raw Data)")

    plt.legend()
    plt.grid(True, linestyle=":", alpha=0.6)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
