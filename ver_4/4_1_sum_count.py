import sqlite3
import shutil
from pathlib import Path

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent
VER3_PATH = TOP_PATH.parent / "ver_3"

SRC_DB = TOP_PATH / "data_4_0.db"
DST_DB = TOP_PATH / "data_4_1.db"
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

CREATE TABLE IF NOT EXISTS live_count_by_time (
    time TEXT PRIMARY KEY,
    avg_live_count REAL NOT NULL,
    cnt_0 INTEGER NOT NULL,
    cnt_1 INTEGER NOT NULL,
    cnt_2 INTEGER NOT NULL,
    cnt_3 INTEGER NOT NULL,
    cnt_4 INTEGER NOT NULL,
    cnt_5 INTEGER NOT NULL,
    cnt_6 INTEGER NOT NULL,
    cnt_7 INTEGER NOT NULL,
    cnt_8 INTEGER NOT NULL,
    cnt_9 INTEGER NOT NULL,
    cnt_10 INTEGER NOT NULL
);

    """)

    conn.commit()
    
    cur.executescript("""

INSERT OR REPLACE INTO live_count_by_time (
    time,
    avg_live_count,
    cnt_0,
    cnt_1, cnt_2, cnt_3, cnt_4, cnt_5,
    cnt_6, cnt_7, cnt_8, cnt_9, cnt_10
)
WITH dist AS (
    -- 各 time 在期間內 live_count = 1..10 出現幾天
    SELECT
        time,
        SUM(CASE WHEN live_count = 1  THEN 1 ELSE 0 END) AS cnt_1,
        SUM(CASE WHEN live_count = 2  THEN 1 ELSE 0 END) AS cnt_2,
        SUM(CASE WHEN live_count = 3  THEN 1 ELSE 0 END) AS cnt_3,
        SUM(CASE WHEN live_count = 4  THEN 1 ELSE 0 END) AS cnt_4,
        SUM(CASE WHEN live_count = 5  THEN 1 ELSE 0 END) AS cnt_5,
        SUM(CASE WHEN live_count = 6  THEN 1 ELSE 0 END) AS cnt_6,
        SUM(CASE WHEN live_count = 7  THEN 1 ELSE 0 END) AS cnt_7,
        SUM(CASE WHEN live_count = 8  THEN 1 ELSE 0 END) AS cnt_8,
        SUM(CASE WHEN live_count = 9  THEN 1 ELSE 0 END) AS cnt_9,
        SUM(CASE WHEN live_count = 10 THEN 1 ELSE 0 END) AS cnt_10
    FROM live_concurrent
    WHERE
        (date = '2025-06-29' AND time >= '12:00')
        OR
        (date > '2025-06-29' AND date < '2025-12-26')
        OR
        (date = '2025-12-26' AND time < '12:00')
    GROUP BY time
)
SELECT
    d.time,

    -- avg = (1*cnt1 + ... + 10*cnt10) / 180
    ROUND(
        (
            1.0 * d.cnt_1 +
            2.0 * d.cnt_2 +
            3.0 * d.cnt_3 +
            4.0 * d.cnt_4 +
            5.0 * d.cnt_5 +
            6.0 * d.cnt_6 +
            7.0 * d.cnt_7 +
            8.0 * d.cnt_8 +
            9.0 * d.cnt_9 +
            10.0 * d.cnt_10
        ) / 180.0,
        2
    ) AS avg_live_count,

    -- cnt_0 = 180 - sum(cnt1..cnt10)
    180
    - (
        d.cnt_1 + d.cnt_2 + d.cnt_3 + d.cnt_4 + d.cnt_5 +
        d.cnt_6 + d.cnt_7 + d.cnt_8 + d.cnt_9 + d.cnt_10
      ) AS cnt_0,

    d.cnt_1,
    d.cnt_2,
    d.cnt_3,
    d.cnt_4,
    d.cnt_5,
    d.cnt_6,
    d.cnt_7,
    d.cnt_8,
    d.cnt_9,
    d.cnt_10

FROM dist d;

    """)

    conn.close()

    print("🎉 已完成只保留子午資料！")

if __name__ == "__main__":
    main()
