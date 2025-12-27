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
        
    """)

    conn.commit()
    
    cur.executescript("""

    """)

    conn.close()

    print("🎉 已完成只保留子午資料！")

if __name__ == "__main__":
    main()
