import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_1_2.db"
DST_DB = TOP_PATH / "data_1_3.db"


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_1_2.db")

    if DST_DB.exists():
        DST_DB.unlink()
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_2_0.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    cur.executescript("""

DROP TABLE IF EXISTS channel_avg;

CREATE TABLE channel_avg (
    channel_id   TEXT PRIMARY KEY,
    channel_name TEXT,

    -- YouTube（raw）
    yt_avg     REAL,   -- 原始算術平均（人數）
    yt_std     REAL,   -- 原始標準差（人數）
    yt_min     REAL,   -- 最小觀看數（人數）
    yt_max     REAL,   -- 最大觀看數（人數）

    -- YouTube（ln）
    yt_ln_avg  REAL,   -- ln 空間平均 = avg(ln(x))
    yt_ln_std  REAL,   -- ln 空間標準差

    -- Twitch（raw）
    tw_avg     REAL,
    tw_std     REAL,
    tw_min     REAL,
    tw_max     REAL,

    -- Twitch（ln）
    tw_ln_avg  REAL,
    tw_ln_std  REAL
);

    """)

    print("📊 建立 channel_avg（以 streamer 順序）")

    cur.execute("""

WITH base AS (
    SELECT
        s.id AS streamer_id,
        s.channel_id,
        s.channel_name,

        -- raw stats
        AVG(CASE WHEN m.yt_number != 0 THEN m.youtube END) AS yt_mean,
        AVG(CASE WHEN m.tw_number != 0 THEN m.twitch END)  AS tw_mean,

        AVG(CASE WHEN m.yt_number != 0 THEN m.youtube * m.youtube END) AS yt_mean2,
        AVG(CASE WHEN m.tw_number != 0 THEN m.twitch * m.twitch END)   AS tw_mean2

    FROM streamer s
    LEFT JOIN main m
        ON m.channel = s.channel_id
    GROUP BY s.id,s.channel_id, s.channel_name
),
ln_part AS (
    SELECT
        b.streamer_id,
        b.channel_id,
        b.channel_name,
        b.yt_mean,
        b.yt_mean2,
        b.tw_mean,
        b.tw_mean2,

        -- ln stats
        AVG(CASE
                WHEN m.yt_number != 0 AND m.youtube > 0
                THEN ln(m.youtube)
            END) AS yt_ln_mean,

        AVG(CASE
                WHEN m.tw_number != 0 AND m.twitch > 0
                THEN ln(m.twitch)
            END) AS tw_ln_mean,

        AVG(CASE
                WHEN m.yt_number != 0 AND m.youtube > 0
                THEN ln(m.youtube) * ln(m.youtube)
            END) AS yt_ln_mean2,

        AVG(CASE
                WHEN m.tw_number != 0 AND m.twitch > 0
                THEN ln(m.twitch) * ln(m.twitch)
            END) AS tw_ln_mean2

    FROM base b
    LEFT JOIN main m
        ON m.channel = b.channel_id
    GROUP BY b.streamer_id,b.channel_id, b.channel_name
)
INSERT INTO channel_avg (
    channel_id, channel_name,

    yt_avg, yt_std,
    yt_ln_avg, yt_ln_std,
    yt_min, yt_max,

    tw_avg, tw_std,
    tw_ln_avg, tw_ln_std,
    tw_min, tw_max
)
SELECT
    channel_id,
    channel_name,

    -- YT raw
    ROUND(yt_mean, 1),
    ROUND(
        sqrt(
            CASE
                WHEN yt_mean2 - yt_mean * yt_mean < 0 THEN 0
                ELSE yt_mean2 - yt_mean * yt_mean
            END
        ), 1
    ),

    -- YT ln
    ROUND(yt_ln_mean, 3),
    ROUND(
        sqrt(
            CASE
                WHEN yt_ln_mean2 - yt_ln_mean * yt_ln_mean < 0 THEN 0
                ELSE yt_ln_mean2 - yt_ln_mean * yt_ln_mean
            END
        ), 3
    ),

    -- YT ±2.5σ → raw
    ROUND(exp(yt_ln_mean - 2.5 * 
        sqrt(
            CASE
                WHEN yt_ln_mean2 - yt_ln_mean * yt_ln_mean < 0 THEN 0
                ELSE yt_ln_mean2 - yt_ln_mean * yt_ln_mean
            END
        )
    ), 1),
    ROUND(exp(yt_ln_mean + 2.5 * 
        sqrt(
            CASE
                WHEN yt_ln_mean2 - yt_ln_mean * yt_ln_mean < 0 THEN 0
                ELSE yt_ln_mean2 - yt_ln_mean * yt_ln_mean
            END
        )
    ), 1),

    -- TW raw
    ROUND(tw_mean, 1),
    ROUND(
        sqrt(
            CASE
                WHEN tw_mean2 - tw_mean * tw_mean < 0 THEN 0
                ELSE tw_mean2 - tw_mean * tw_mean
            END
        ), 1
    ),

    -- TW ln
    ROUND(tw_ln_mean, 3),
    ROUND(
        sqrt(
            CASE
                WHEN tw_ln_mean2 - tw_ln_mean * tw_ln_mean < 0 THEN 0
                ELSE tw_ln_mean2 - tw_ln_mean * tw_ln_mean
            END
        ), 3
    ),

    -- TW ±2.5σ → raw
    ROUND(exp(tw_ln_mean - 2.5 *
        sqrt(
            CASE
                WHEN tw_ln_mean2 - tw_ln_mean * tw_ln_mean < 0 THEN 0
                ELSE tw_ln_mean2 - tw_ln_mean * tw_ln_mean
            END
        )
    ), 1),
    ROUND(exp(tw_ln_mean + 2.5 *
        sqrt(
            CASE
                WHEN tw_ln_mean2 - tw_ln_mean * tw_ln_mean < 0 THEN 0
                ELSE tw_ln_mean2 - tw_ln_mean * tw_ln_mean
            END
        )
    ), 1)

FROM ln_part
ORDER BY streamer_id;


    """)

    conn.commit()
    print("✅ channel_avg 建立完成（順序與 streamer 一致）")

    conn.close()
    print("\n🎉 data_1_3 完成（已對應 streamer）")


if __name__ == "__main__":
    main()
