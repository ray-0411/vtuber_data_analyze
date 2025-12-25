import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent
VER2_PATH = TOP_PATH
VER1_PATH = TOP_PATH.parent / "ver_1"

SRC_DB = VER1_PATH / "data_1_5.db"   # 來源：已有 channel_avg
DST_DB = VER2_PATH / "data_2_1.db"   # 輸出：2.1 結果

def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_2_0.db")

    if DST_DB.exists():
        DST_DB.unlink()
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_2_1.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # === 建 time_slots ===
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

    # === 建 yt_time_profile ===
    # =========================
    # 建立 2.1 YT 分析表
    # =========================
    cur.execute("""
    CREATE TABLE yt_time_profile (
        channel_id TEXT,
        channel_name TEXT,
        time TEXT,

        live_count INTEGER,
        avg_viewers REAL,
        diff_percent REAL,

        PRIMARY KEY (channel_id, time)
    );
    """)

    print("📊 計算 2.1 YT 時間分佈（含差異百分比）")

    cur.execute("""
    INSERT OR REPLACE INTO yt_time_profile
    SELECT
        c.channel_id,
        c.channel_name,
        t.time,

        COUNT(m.youtube) AS live_count,
        COALESCE(ROUND(AVG(m.youtube), 1), 0) AS avg_viewers,

        CASE
            WHEN c.yt_avg = 0 THEN 0
            ELSE ROUND(
                (COALESCE(AVG(m.youtube), 0) - c.yt_avg)
                / c.yt_avg * 100,
                2
            )
        END AS diff_percent

    FROM channel_avg c
    CROSS JOIN time_slots t
    LEFT JOIN main m
        ON m.channel = c.channel_id
    AND m.time = t.time
    AND m.yt_number != 0
    WHERE c.yt_avg <> 0
    GROUP BY c.channel_id, c.channel_name, t.time
    ORDER BY c.channel_id, t.time;
    """)

    conn.commit()
    conn.close()
    print("🎉 data_2_1 完成（YT 時間分佈）")

if __name__ == "__main__":
    main()
