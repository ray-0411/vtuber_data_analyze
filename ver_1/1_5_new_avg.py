import sqlite3
import shutil
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_1_4.db"
DST_DB = TOP_PATH / "data_1_5.db"


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_1_3.db")

    if DST_DB.exists():
        DST_DB.unlink()
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_1_4.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    print("🧹 重新建立 channel_avg（cleaned main）")

    # ─────────────────────────────
    # 重建表
    # ─────────────────────────────
    cur.execute("DROP TABLE IF EXISTS channel_avg")
    cur.execute("""

    CREATE TABLE channel_avg (
        channel_id   TEXT PRIMARY KEY,
        channel_name TEXT,

        yt_avg REAL,
        yt_std REAL,
        yt_geo_avg REAL,
        yt_log_geo_avg REAL,

        tw_avg REAL,
        tw_std REAL,
        tw_geo_avg REAL,
        tw_log_geo_avg REAL
    );

    """)

    print("📊 重新計算 avg / std")

    # ─────────────────────────────
    # 重新計算（母體標準差）
    # ─────────────────────────────
    cur.execute("""
    INSERT INTO channel_avg (
        channel_id, channel_name,

        yt_avg, yt_std,
        yt_geo_avg, yt_log_geo_avg,

        tw_avg, tw_std,
        tw_geo_avg, tw_log_geo_avg
    )
    SELECT
        s.channel_id,
        s.channel_name,

        -- ───────── YT ─────────

        -- 算術平均
        COALESCE(
            ROUND(AVG(CASE WHEN m.yt_number != 0 THEN m.youtube END), 1),
            0
        ) AS yt_avg,

        -- 母體標準差
        COALESCE(
            ROUND(
                sqrt(
                    AVG(CASE WHEN m.yt_number != 0 THEN m.youtube * m.youtube END)
                  - AVG(CASE WHEN m.yt_number != 0 THEN m.youtube END)
                    * AVG(CASE WHEN m.yt_number != 0 THEN m.youtube END)
                ),
                1
            ),
            0
        ) AS yt_std,

        -- 幾何平均（log-space）
        COALESCE(
            ROUND(
                AVG(CASE WHEN m.yt_number != 0 AND m.youtube > 0
                         THEN ln(m.youtube) END),
                6
            ),
            NULL
        ) AS yt_log_geo_avg,

        -- 幾何平均（還原）
        COALESCE(
            ROUND(
                exp(
                    AVG(CASE WHEN m.yt_number != 0 AND m.youtube > 0
                             THEN ln(m.youtube) END)
                ),
                1
            ),
            0
        ) AS yt_geo_avg,

        -- ───────── TW ─────────

        -- 算術平均
        COALESCE(
            ROUND(AVG(CASE WHEN m.tw_number != 0 THEN m.twitch END), 1),
            0
        ) AS tw_avg,

        -- 母體標準差
        COALESCE(
            ROUND(
                sqrt(
                    AVG(CASE WHEN m.tw_number != 0 THEN m.twitch * m.twitch END)
                  - AVG(CASE WHEN m.tw_number != 0 THEN m.twitch END)
                    * AVG(CASE WHEN m.tw_number != 0 THEN m.twitch END)
                ),
                1
            ),
            0
        ) AS tw_std,

        -- 幾何平均（log-space）
        COALESCE(
            ROUND(
                AVG(CASE WHEN m.tw_number != 0 AND m.twitch > 0
                         THEN ln(m.twitch) END),
                6
            ),
            NULL
        ) AS tw_log_geo_avg,

        -- 幾何平均（還原）
        COALESCE(
            ROUND(
                exp(
                    AVG(CASE WHEN m.tw_number != 0 AND m.twitch > 0
                             THEN ln(m.twitch) END)
                ),
                1
            ),
            0
        ) AS tw_geo_avg

    FROM streamer s
    LEFT JOIN main m
        ON m.channel = s.channel_id
    GROUP BY s.channel_id, s.channel_name
    ORDER BY s.id;
""")

    conn.commit()
    conn.close()

    print("✅ 1_5 channel_avg 重新計算完成（cleaned）")


if __name__ == "__main__":
    main()
