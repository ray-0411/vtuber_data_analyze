import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

# ====== Path 設定 ======
TOP_PATH = Path(__file__).resolve().parent
VER1_PATH = TOP_PATH.parent / "ver_1"

SRC_DB = VER1_PATH / "data_1_2.db"
DST_DB = TOP_PATH / "data_3_0.db"
# =======================


def to_dt(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")


def expected_points(start_dt, end_dt):
    minutes = int((end_dt - start_dt).total_seconds() // 60)
    return minutes // 15 + 1


def process_platform(cur, platform):
    """
    platform = 'YT' or 'TW'
    """
    if platform == "YT":
        id_col = "yt_number"
        view_col = "youtube"
    else:
        id_col = "tw_number"
        view_col = "twitch"

    print(f"\n📊 開始分析 {platform}")

    cur.execute(f"""
        SELECT channel, {id_col}, date, time, {view_col}
        FROM main
        WHERE {id_col} != 0
        ORDER BY  {id_col}, channel, date, time
    """)
    rows = cur.fetchall()

    streams = {}
    for channel, sid, d, t, v in rows:
        streams.setdefault((channel, sid), []).append((d, t, v))

    print(f"  🔍 找到 {len(streams)} 個 stream")

    for i, ((channel, sid), records) in enumerate(streams.items(), 1):
        dts = [(to_dt(d, t), v) for d, t, v in records]

        times = [dt for dt, _ in dts]
        viewers = [v for _, v in dts]

        start_dt = min(times)
        end_dt = max(times)

        avg_v = round(sum(viewers) / len(viewers), 1)

        max_v = max(viewers)
        min_v = min(viewers)

        max_time = min(dt for dt, v in dts if v == max_v)
        min_time = min(dt for dt, v in dts if v == min_v)

        actual = len(dts)
        expect = expected_points(start_dt, end_dt)
        missing = expect - actual

        cur.execute("""
            INSERT INTO stream_analysis (
                platform, stream_id, channel,
                start_time, end_time,
                avg_viewers,
                max_viewers, max_time,
                min_viewers, min_time,
                expected_points, actual_points, missing_points
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            platform,
            sid,
            channel,
            start_dt.strftime("%Y-%m-%d %H:%M"),
            end_dt.strftime("%Y-%m-%d %H:%M"),
            avg_v,
            max_v,
            max_time.strftime("%Y-%m-%d %H:%M"),
            min_v,
            min_time.strftime("%Y-%m-%d %H:%M"),
            expect,
            actual,
            missing
        ))

        if i % 100 == 0 or i == len(streams):
            print(f"    ⏳ {i}/{len(streams)} streams 完成")

    print(f"✅ {platform} 分析完成")


def main():
    if not SRC_DB.exists():
        raise FileNotFoundError("找不到 data_1_2.db")

    if DST_DB.exists():
        DST_DB.unlink()   # 直接刪掉舊檔
        print(f"⚠️ 已刪除舊的 {DST_DB.name}")

    shutil.copyfile(SRC_DB, DST_DB)
    print("✅ 建立 data_2_0.db")

    conn = sqlite3.connect(DST_DB)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stream_analysis (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT NOT NULL,
        stream_id INTEGER NOT NULL,
        channel TEXT NOT NULL,
        start_time TEXT NOT NULL,
        end_time   TEXT NOT NULL,
        avg_viewers REAL NOT NULL,
        max_viewers INTEGER NOT NULL,
        max_time TEXT NOT NULL,
        min_viewers INTEGER NOT NULL,
        min_time TEXT NOT NULL,
        expected_points INTEGER NOT NULL,
        actual_points   INTEGER NOT NULL,
        missing_points  INTEGER NOT NULL
    );
    """)
    conn.commit()

    process_platform(cur, "YT")
    process_platform(cur, "TW")

    conn.commit()
    conn.close()

    print("\n🎉 data_2_0 分析完成，stream_analysis 可直接使用")


if __name__ == "__main__":
    main()
