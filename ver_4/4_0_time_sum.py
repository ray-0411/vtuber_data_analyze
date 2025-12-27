import sqlite3
import shutil
from pathlib import Path

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent
VER3_PATH = TOP_PATH.parent / "ver_3"

SRC_DB = VER3_PATH / "data_3_2.db"
DST_DB = TOP_PATH / "data_4_0.db"
# =======================


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError(f"找不到來源資料庫：{SRC_DB}")

    if DST_DB.exists():
        DST_DB.unlink()   # 直接刪掉舊檔
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    # 1️⃣ 複製 DB
    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 已建立 data_4_0.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    cur.executescript("""

CREATE TABLE IF NOT EXISTS live_concurrent (
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    live_count INTEGER NOT NULL,
    id_list TEXT NOT NULL,

    -- 同時直播的幾何平均表現（%）
    geo_perf_percent REAL,

    PRIMARY KEY (date, time)
);

DROP TABLE IF EXISTS concurrent_effect;

CREATE TABLE concurrent_effect (
    live_count INTEGER PRIMARY KEY,
    avg_geo_perf_percent REAL NOT NULL,
    sample_cnt INTEGER NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

    """)

    conn.commit()
    
    cur.executescript("""

INSERT INTO live_concurrent (
    date, time,
    live_count,
    id_list,
    geo_perf_percent
)
SELECT
    m.date,
    m.time,

    COUNT(m.id) AS live_count,
    GROUP_CONCAT(m.id) AS id_list,

    -- 幾何平均表現（%）
    (
        exp(
            AVG(
                CASE
                    WHEN m.yt_number != 0 AND m.youtube > 0
                        THEN ln(m.youtube) - c.yt_log_geo_avg
                    WHEN m.tw_number != 0 AND m.twitch > 0
                        THEN ln(m.twitch) - c.tw_log_geo_avg
                END
            )
        ) - 1
    ) * 100 AS geo_perf_percent

FROM main m
JOIN channel_avg c
    ON m.channel = c.channel_id
WHERE
    m.yt_number IS NOT NULL
    OR m.tw_number IS NOT NULL
GROUP BY
    m.date, m.time;


    """)
    conn.commit()
    
    cur.executescript("""
    
INSERT INTO concurrent_effect (
    live_count,
    avg_geo_perf_percent,
    sample_cnt
)
SELECT
    live_count,

    -- 幾何平均（log-space 平均再還原）
    (exp(AVG(ln(1 + geo_perf_percent / 100.0))) - 1) * 100
        AS avg_geo_perf_percent,

    COUNT(*) AS sample_cnt
FROM live_concurrent
WHERE geo_perf_percent IS NOT NULL
GROUP BY live_count
ORDER BY live_count;

    
    """)
    conn.close()

    print("🎉 已完成只保留子午資料！")

if __name__ == "__main__":
    main()
