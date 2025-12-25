import sqlite3
import shutil       
from pathlib import Path

TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_1_3.db"
DST_DB = TOP_PATH / "data_1_4.db"


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



    print("🧹 刪除 YT 超過 ±2.5σ（ln）的資料")

    cur.execute("""
        DELETE FROM main
        WHERE id IN (
            SELECT
                m.id
            FROM main m
            JOIN channel_avg c
                ON c.channel_id = m.channel
            WHERE
                m.youtube > 0
                AND c.yt_ln_std > 0
                AND ABS(
                    (ln(m.youtube) - c.yt_ln_avg) / c.yt_ln_std
                ) > 2.5
        );
    """)

    print(f"   → 影響筆數（YT）：{cur.rowcount}")


    print("🧹 刪除 TW 超過 ±2.5σ（ln）的資料")

    cur.execute("""
        DELETE FROM main
        WHERE id IN (
            SELECT
                m.id
            FROM main m
            JOIN channel_avg c
                ON c.channel_id = m.channel
            WHERE
                m.twitch > 0
                AND c.tw_ln_std > 0
                AND ABS(
                    (ln(m.twitch) - c.tw_ln_avg) / c.tw_ln_std
                ) > 2.5
        );
    """)

    print(f"   → 影響筆數（TW）：{cur.rowcount}")

    conn.commit()
    conn.close()

    print("\n✅ 1_4 main 異常值刪除完成")


if __name__ == "__main__":
    main()
