import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent
VER2_PATH = TOP_PATH

SRC_DB = VER2_PATH / "data_2_1.db"   # 來源：已有 channel_avg
DST_DB = VER2_PATH / "data_2_2.db"   # 輸出：2.2 結果（同一個 DB）

def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_2_2.db")

    if not DST_DB.exists():
        shutil.copyfile(SRC_DB, DST_DB)
        print("✅ 建立 data_2_2.db（from data_2_1）")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # === 建 time_slots（若已存在不會重建）===
    cur.execute("""
    CREATE TABLE IF NOT EXISTS time_slots (
        time TEXT PRIMARY KEY
    );
    """)

    cur.execute("SELECT COUNT(*) FROM time_slots;")
    if cur.fetchone()[0] == 0:
        for h in range(24):
            for m in (0, 15, 30, 45):
                cur.execute(
                    "INSERT INTO time_slots VALUES (?)",
                    (f"{h:02d}:{m:02d}",)
                )

    # === 建 tw_time_profile ===
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tw_time_profile (
        channel_id TEXT,
        channel_name TEXT,
        time TEXT,

        live_count INTEGER,
        avg_viewers REAL,
        diff_percent REAL,

        PRIMARY KEY (channel_id, time)
    );
    """)

    print("📊 計算 2.2 TW 時間分佈（含差異百分比）")

    cur.execute("""
    INSERT OR REPLACE INTO tw_time_profile
    SELECT
        c.channel_id,
        c.channel_name,
        t.time,

        COUNT(m.twitch) AS live_count,
        COALESCE(ROUND(AVG(m.twitch), 1), 0) AS avg_viewers,

        CASE
            WHEN c.tw_avg = 0 THEN 0
            ELSE ROUND(
                (COALESCE(AVG(m.twitch), 0) - c.tw_avg)
                / c.tw_avg * 100,
                2
            )
        END AS diff_percent

    FROM channel_avg c
    CROSS JOIN time_slots t
    LEFT JOIN main m
        ON m.channel = c.channel_id
       AND m.time = t.time
       AND m.tw_number != 0
    WHERE c.tw_avg <> 0
    GROUP BY c.channel_id, c.channel_name, t.time
    ORDER BY c.channel_id, t.time;
    """)

    conn.commit()
    conn.close()

    print("🎉 data_2_2 完成（TW 時間分佈）")


if __name__ == "__main__":
    main()
