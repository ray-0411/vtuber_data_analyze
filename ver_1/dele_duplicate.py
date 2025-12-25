import sqlite3
import shutil
from pathlib import Path

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH / "data_1_1.db"
DST_DB = TOP_PATH / "data_1_2.db"
# =======================


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError(f"找不到來源資料庫：{SRC_DB}")

    if DST_DB.exists():
        raise FileExistsError(f"{DST_DB} 已存在，請確認是否要覆蓋")

    # 1️⃣ 複製 DB
    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 已建立 data_1_2.db（準備去重複）")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # 2️⃣ 先看看會刪掉幾筆（安心用）
    cur.execute("""
        SELECT COUNT(*)
        FROM "main"
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM "main"
            GROUP BY
                date,
                time,
                channel,
                yt_number,
                tw_number,
                youtube,
                twitch
        );
    """)
    delete_count = cur.fetchone()[0]
    print(f"🧹 預計刪除重複筆數：{delete_count}")

    # 3️⃣ 刪除重複資料（核心）
    cur.execute("""
        DELETE FROM "main"
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM "main"
            GROUP BY
                date,
                time,
                channel,
                yt_number,
                tw_number,
                youtube,
                twitch
        );
    """)

    conn.commit()

    # 4️⃣ 剩餘筆數
    cur.execute('SELECT COUNT(*) FROM "main"')
    remain = cur.fetchone()[0]
    print(f"📊 去重後剩餘 main 筆數：{remain}")

    conn.close()
    print("🎉 去重複完成，data_1_2.db 準備好分析")


if __name__ == "__main__":
    main()
