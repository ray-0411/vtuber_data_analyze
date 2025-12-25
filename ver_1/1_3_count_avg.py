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

    cur.execute("""
    CREATE TABLE channel_avg (
        channel_id   TEXT PRIMARY KEY,
        channel_name TEXT,
        yt_avg REAL,
        tw_avg REAL
    );
    """)

    print("📊 建立 channel_avg（以 streamer 順序）")

    cur.execute("""
    INSERT INTO channel_avg (channel_id, channel_name, yt_avg, tw_avg)
    SELECT
        s.channel_id,
        s.channel_name,
        COALESCE(
        ROUND(AVG(CASE WHEN m.yt_number != 0 THEN m.youtube END), 1),
            0
        ) AS yt_avg,
        COALESCE(
            ROUND(AVG(CASE WHEN m.tw_number != 0 THEN m.twitch END), 1),
            0
        ) AS tw_avg
    FROM streamer s
    LEFT JOIN main m
        ON m.channel = s.channel_id
    GROUP BY s.channel_id, s.channel_name
    ORDER BY s.id;
    """)

    conn.commit()
    print("✅ channel_avg 建立完成（順序與 streamer 一致）")

    conn.close()
    print("\n🎉 data_2_0 完成（已對應 streamer）")


if __name__ == "__main__":
    main()
