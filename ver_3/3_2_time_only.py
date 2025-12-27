import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_3_1.db"   # 來源：已有 channel_avg
DST_DB = TOP_PATH / "data_3_2.db"   # 輸出：2.1 結果

def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_2_0.db")

    if DST_DB.exists():
        DST_DB.unlink()
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_3_2.db")

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
    cur.executescript("""
    CREATE TABLE yt_time_profile (
        channel_id TEXT,
        channel_name TEXT,
        time TEXT,

        live_count INTEGER,
        avg_viewers REAL,
        diff_percent REAL,

        PRIMARY KEY (channel_id, time)
    );
    CREATE TABLE tw_time_profile (
        channel_id TEXT,
        channel_name TEXT,
        time TEXT,

        live_count INTEGER,
        avg_viewers REAL,
        diff_percent REAL,

        PRIMARY KEY (channel_id, time)
    );
    """)

    print("📊 計算 3.2 YT 時間分佈（含差異百分比）")

    cur.execute("""
    INSERT OR REPLACE INTO yt_time_profile
    SELECT
        c.channel_id,
        c.channel_name,
        t.time,

        COUNT(m.youtube) AS live_count,
        COALESCE(ROUND(AVG(m.youtube), 1), 0) AS avg_viewers,

        CASE
            WHEN c.yt_avg <= 0 THEN 0
            WHEN AVG(m.youtube) <= 0 THEN 0
            ELSE ROUND(
                (
                    exp(
                        AVG(ln(m.youtube)) - c.yt_log_geo_avg
                    ) - 1
                ) * 100,
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
    
    print("🎉 完成（YT 時間分佈）")
    
    
    print("📊 計算 3.2 TW 時間分佈（含差異百分比）")
    cur.execute("""
    INSERT OR REPLACE INTO tw_time_profile
    SELECT
        c.channel_id,
        c.channel_name,
        t.time,

        COUNT(m.twitch) AS live_count,
        COALESCE(ROUND(AVG(m.twitch), 1), 0) AS avg_viewers,

        CASE
            WHEN c.tw_avg <= 0 THEN 0
            WHEN AVG(m.twitch) <= 0 THEN 0
            ELSE ROUND(
                (
                    exp(
                        AVG(ln(m.twitch)) - c.tw_log_geo_avg
                    ) - 1
                ) * 100,
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
    print("🎉 完成（TW 時間分佈）")
    
    cur.executescript("""

CREATE TABLE time_global_profile AS
WITH yt AS (
    SELECT
        time,
        SUM(live_count) AS yt_sum,
        SUM(avg_viewers * live_count) AS yt_avg_wsum,

        -- 🔑 即時還原 diff_log
        SUM(
            ln(1 + diff_percent / 100.0) * live_count
        ) AS yt_diff_log_wsum
    FROM yt_time_profile
    GROUP BY time
),
tw AS (
    SELECT
        time,
        SUM(live_count) AS tw_sum,
        SUM(avg_viewers * live_count) AS tw_avg_wsum,
        SUM(
            ln(1 + diff_percent / 100.0) * live_count
        ) AS tw_diff_log_wsum
    FROM tw_time_profile
    GROUP BY time
),
all_time AS (
    SELECT time FROM yt
    UNION
    SELECT time FROM tw
)
SELECT
    a.time,

    -- ───────── YT ─────────
    yt.yt_sum,

    ROUND(
        CASE
            WHEN yt.yt_sum > 0
            THEN yt.yt_avg_wsum / yt.yt_sum
            ELSE NULL
        END
    , 2) AS yt_weighted_avg,

    ROUND(
        CASE
            WHEN yt.yt_sum > 0
            THEN (
                exp(yt.yt_diff_log_wsum * 1.0 / yt.yt_sum) - 1
            ) * 100
            ELSE NULL
        END
    , 2) AS yt_weighted_diff,

    -- ───────── TW ─────────
    tw.tw_sum,

    ROUND(
        CASE
            WHEN tw.tw_sum > 0
            THEN tw.tw_avg_wsum / tw.tw_sum
            ELSE NULL
        END
    , 2) AS tw_weighted_avg,

    ROUND(
        CASE
            WHEN tw.tw_sum > 0
            THEN (
                exp(tw.tw_diff_log_wsum * 1.0 / tw.tw_sum) - 1
            ) * 100
            ELSE NULL
        END
    , 2) AS tw_weighted_diff

FROM all_time a
LEFT JOIN yt ON yt.time = a.time
LEFT JOIN tw ON tw.time = a.time
ORDER BY a.time;


    """)
    
    
    conn.close()
    
    print("🎉 data_3_2 完成（YT & TW 時間分佈）")
    


if __name__ == "__main__":
    main()
