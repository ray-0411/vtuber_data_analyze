import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_1_0.db"
DST_DB = TOP_PATH / "data_1_1.db"


def to_15min_block_hhmm(time_str: str) -> str:
    """
    time_str: HH:MM:SS
    return:   HH:MM  (MM ∈ {00,15,30,45})
    """
    h, m, s = map(int, time_str.split(":"))
    block_min = (m // 15) * 15
    return f"{h:02d}:{block_min:02d}"



def main():
    if not SRC_DB.exists():
        raise FileNotFoundError(f"找不到來源資料庫：{SRC_DB}")

    shutil.copyfile(SRC_DB, DST_DB)
    
    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # 1️⃣ 先算所有資料的 time_block
    cur.execute('SELECT id, time FROM "main"')
    rows = cur.fetchall()
    total = len(rows)

    print(f"🕒 開始時間離散化，共 {total} 筆")
    
    for i, (rid, time_str) in enumerate(rows, 1):
        new_time = to_15min_block_hhmm(time_str)
        cur.execute(
            'UPDATE "main" SET time = ? WHERE id = ?',
            (new_time, rid)
        )
        
        if i % 2000 == 0 or i == total:
            print(f"  ⏳ 已處理 {i}/{total} 筆")

    conn.commit()
    
    print("✅ 時間離散化完成")
    
    print("⚙️ 建立 index（YouTube / Twitch）")

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_main_yt
    ON "main"(date, time, yt_number);
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_main_tw
    ON "main"(date, time, tw_number);
    """)

    conn.commit()
    print("✅ index 建立完成")
    
    cur.execute("ANALYZE;")
    conn.commit()
    print("📊 ANALYZE 完成")

    
    print("📊 開始計算 YouTube 平均（一次 UPDATE）")

    cur.execute("""
    UPDATE "main"
    SET youtube = (
        SELECT CAST(AVG(youtube) AS INTEGER)
        FROM "main" m2
        WHERE
            m2.date = "main".date
            AND m2.time = "main".time
            AND m2.yt_number = "main".yt_number
    )
    WHERE yt_number != 0;
    """)

    conn.commit()
    print("✅ YouTube 平均完成")


    print("📊 開始計算 Twitch 平均（一次 UPDATE）")

    cur.execute("""
    UPDATE "main"
    SET twitch = (
        SELECT CAST(AVG(twitch) AS INTEGER)
        FROM "main" m2
        WHERE
            m2.date = "main".date
            AND m2.time = "main".time
            AND m2.tw_number = "main".tw_number
    )
    WHERE tw_number != 0;
    """)

    conn.commit()
    print("✅ Twitch 平均完成")
    
    

    conn.close()
    print("✅ 15 分鐘重取樣完成")


if __name__ == "__main__":
    main()
