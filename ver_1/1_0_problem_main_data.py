import sqlite3
import shutil
from pathlib import Path

# ====== 設定 ======
TOP_PATH = Path(__file__).resolve().parent

SRC_DB = TOP_PATH.parent / Path("data.db")        # 原始資料庫
DST_DB = TOP_PATH / Path("data_1_0.db")    # 清洗後資料庫

DELETE_SQL = """
DELETE FROM "main"
WHERE
    (yt_number = 0 AND tw_number = 0)
    OR
    (youtube < 10 AND twitch < 10);
"""

COUNT_SQL = """
SELECT COUNT(*) FROM "main"
WHERE
    (yt_number = 0 AND tw_number = 0)
    OR
    (youtube < 10 AND twitch < 10);
"""
# ==================


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError(f"找不到資料庫：{SRC_DB}")

    # 1️⃣ 複製資料庫（不動原始）
    shutil.copyfile(SRC_DB, DST_DB)
    print(f"✅ 已建立 {DST_DB}")

    # 2️⃣ 連線新資料庫
    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    # 3️⃣ 先看看會刪幾筆
    cur.execute(COUNT_SQL)
    delete_count = cur.fetchone()[0]
    print(f"🧹 預計刪除筆數：{delete_count}")

    # 4️⃣ 執行刪除
    cur.execute(DELETE_SQL)
    conn.commit()

    # 5️⃣ 剩餘筆數
    cur.execute('SELECT COUNT(*) FROM "main"')
    remain = cur.fetchone()[0]
    print(f"📊 刪除後剩餘 main 筆數：{remain}")

    conn.close()
    print("🎉 清洗完成")


if __name__ == "__main__":
    main()
