import sqlite3
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from pathlib import Path

# =========================
# 基本設定
# =========================
st.set_page_config(
    page_title="Live Competition Analysis",
    layout="wide"
)

TOP_PATH = Path(__file__).resolve().parent
DB_PATH = TOP_PATH / "data_4_1.db"


def load_df(query):
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn)


# =========================
# 標題
# =========================
st.title("📺 同時直播競爭分析（Live Competition Analysis）")

st.markdown("""
本頁面分析 **不同時間點的同時直播數結構**，
以及其對 **單場直播表現的預期影響**。
""")

# =========================
# 圖 1：時間 × 平均同時直播數
# =========================
st.header("① 各時間點的平均同時直播數")

df_avg = load_df("""
    SELECT time, avg_live_count
    FROM live_count_by_time
    ORDER BY time;
""")

fig1, ax1 = plt.subplots(figsize=(10, 4))
ax1.plot(df_avg["time"], df_avg["avg_live_count"])
ax1.set_xlabel("Time")
ax1.set_ylabel("Average Concurrent Live Count")
ax1.set_title("Average Concurrent Live Count by Time")
ax1.tick_params(axis="x", rotation=90)

st.pyplot(fig1)

st.markdown("""
**解讀方式**  
- 數值越高 → 該時段競爭越激烈  
- 可對照後續圖表觀察競爭是否轉為表現壓力
""")

# =========================
# 圖 2：時間 × live_count 分布（Heatmap）
# =========================
st.header("② 各時間點的同時直播數分布（Heatmap）")

df_dist = load_df("""
    SELECT time, 0 AS live_count, cnt_0 AS cnt FROM live_count_by_time
    UNION ALL SELECT time, 1, cnt_1 FROM live_count_by_time
    UNION ALL SELECT time, 2, cnt_2 FROM live_count_by_time
    UNION ALL SELECT time, 3, cnt_3 FROM live_count_by_time
    UNION ALL SELECT time, 4, cnt_4 FROM live_count_by_time
    UNION ALL SELECT time, 5, cnt_5 FROM live_count_by_time
    UNION ALL SELECT time, 6, cnt_6 FROM live_count_by_time
    UNION ALL SELECT time, 7, cnt_7 FROM live_count_by_time
    UNION ALL SELECT time, 8, cnt_8 FROM live_count_by_time
    UNION ALL SELECT time, 9, cnt_9 FROM live_count_by_time
    UNION ALL SELECT time, 10, cnt_10 FROM live_count_by_time
    ORDER BY time, live_count;
""")

pivot = df_dist.pivot(
    index="time",
    columns="live_count",
    values="cnt"
).fillna(0)

fig2, ax2 = plt.subplots(figsize=(10, 6))
im = ax2.imshow(pivot.values, aspect="auto")

ax2.set_xticks(range(len(pivot.columns)))
ax2.set_xticklabels(pivot.columns)
ax2.set_yticks(range(len(pivot.index)))
ax2.set_yticklabels(pivot.index)

ax2.set_xlabel("Concurrent Live Count")
ax2.set_ylabel("Time")
ax2.set_title("Distribution of Concurrent Live Count by Time")

plt.colorbar(im, ax=ax2, label="Days")

st.pyplot(fig2)

st.markdown("""
**解讀方式**  
- 顏色越深 → 該時段出現該直播數的天數越多  
- 可快速辨識「結構上競爭集中的時段」
""")

# =========================
# 圖 3：時間 × 預期表現壓力
# =========================
st.header("③ 各時間點的預期直播表現壓力（%）")

df_effect = load_df("""
    SELECT live_count, avg_geo_perf_percent
    FROM concurrent_effect;
""")

df_expected = load_df("""
    SELECT
        t.time,
        (
            t.cnt_0 * 0
            + t.cnt_1 * e1.avg_geo_perf_percent
            + t.cnt_2 * e2.avg_geo_perf_percent
            + t.cnt_3 * e3.avg_geo_perf_percent
            + t.cnt_4 * e4.avg_geo_perf_percent
            + t.cnt_5 * e5.avg_geo_perf_percent
            + t.cnt_6 * e6.avg_geo_perf_percent
            + t.cnt_7 * e7.avg_geo_perf_percent
            + t.cnt_8 * e8.avg_geo_perf_percent
            + t.cnt_9 * e9.avg_geo_perf_percent
            + t.cnt_10 * e10.avg_geo_perf_percent
        ) / 180.0 AS expected_perf
    FROM live_count_by_time t
    LEFT JOIN concurrent_effect e1  ON e1.live_count  = 1
    LEFT JOIN concurrent_effect e2  ON e2.live_count  = 2
    LEFT JOIN concurrent_effect e3  ON e3.live_count  = 3
    LEFT JOIN concurrent_effect e4  ON e4.live_count  = 4
    LEFT JOIN concurrent_effect e5  ON e5.live_count  = 5
    LEFT JOIN concurrent_effect e6  ON e6.live_count  = 6
    LEFT JOIN concurrent_effect e7  ON e7.live_count  = 7
    LEFT JOIN concurrent_effect e8  ON e8.live_count  = 8
    LEFT JOIN concurrent_effect e9  ON e9.live_count  = 9
    LEFT JOIN concurrent_effect e10 ON e10.live_count = 10
    ORDER BY t.time;
""")

fig3, ax3 = plt.subplots(figsize=(10, 4))
ax3.plot(df_expected["time"], df_expected["expected_perf"])
ax3.axhline(0, linestyle="--", linewidth=1)

ax3.set_xlabel("Time")
ax3.set_ylabel("Expected Performance Change (%)")
ax3.set_title("Expected Performance Pressure by Time")
ax3.tick_params(axis="x", rotation=90)

st.pyplot(fig3)

st.markdown("""
**解讀方式**  
- 負值：結構上預期被分流  
- 越負：競爭越激烈  
- 可直接作為「開台時段建議」的依據
""")
