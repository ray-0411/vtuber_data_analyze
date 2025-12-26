import sqlite3
import shutil
from pathlib import Path

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent
VER1_PATH = TOP_PATH.parent / "ver_1"

SRC_DB = VER1_PATH / "data_1_3.db"
DST_DB = TOP_PATH / "data_3_0.db"
# =======================


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError(f"找不到來源資料庫：{SRC_DB}")

    if DST_DB.exists():
        DST_DB.unlink()   # 直接刪掉舊檔
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    # 1️⃣ 複製 DB
    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 已建立 data_3_0.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    cur.executescript("""

CREATE TABLE main_only AS
SELECT m.*
FROM main m
JOIN streamer s
    ON m.channel = s.channel_id
WHERE s."group" = '子午';

DROP TABLE main;

ALTER TABLE main_only RENAME TO main;

    """)

    conn.commit()
    
    cur.executescript("""

DELETE FROM streamer
WHERE "group" != '子午';

DELETE FROM channel_avg
WHERE channel_id NOT IN (
    SELECT DISTINCT channel
    FROM main
);

    """)

    conn.close()

    print("🎉 已完成只保留子午資料！")

if __name__ == "__main__":
    main()
