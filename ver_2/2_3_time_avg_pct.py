import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent
VER2_PATH = TOP_PATH

SRC_DB = VER2_PATH / "data_2_2.db"
DST_DB = VER2_PATH / "data_2_3.db"

def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_2_2.db")

    if DST_DB.exists():
        DST_DB.unlink()
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_2_3.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # 建表
    cur.execute("""
    CREATE TABLE IF NOT EXISTS yt_tw_time_avg (
        time TEXT PRIMARY KEY,
        yt_avg  REAL,
        tw_avg  REAL,
        all_avg REAL
    );
    """)

    print("📊 計算 2.3（YT / TW / ALL 加權平均）")

    cur.execute("""
    INSERT OR REPLACE INTO yt_tw_time_avg
    SELECT
        t.time,

        COALESCE(
            ROUND(
                SUM(yt.diff_percent * yt.live_count)
                / NULLIF(SUM(yt.live_count), 0),
                2
            ),
            0
        ),

        COALESCE(
            ROUND(
                SUM(tw.diff_percent * tw.live_count)
                / NULLIF(SUM(tw.live_count), 0),
                2
            ),
            0
        ),

        COALESCE(
            ROUND(
                (
                    COALESCE(SUM(yt.diff_percent * yt.live_count), 0)
                  + COALESCE(SUM(tw.diff_percent * tw.live_count), 0)
                )
                /
                NULLIF(
                    COALESCE(SUM(yt.live_count), 0)
                  + COALESCE(SUM(tw.live_count), 0),
                    0
                ),
                2
            ),
            0
        )

    FROM time_slots t
    LEFT JOIN yt_time_profile yt
        ON yt.time = t.time
       AND yt.diff_percent <> -100
    LEFT JOIN tw_time_profile tw
        ON tw.time = t.time
       AND tw.diff_percent <> -100
    GROUP BY t.time
    ORDER BY t.time;
    """)

    conn.commit()
    conn.close()

    print("🎉 data_2_3 完成（YT / TW / ALL）")


if __name__ == "__main__":
    main()
