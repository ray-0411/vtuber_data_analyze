import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

#streamlit run ver_3/3_4_all_time_graph.py

# ─────────────────────────────
# 基本設定
# ─────────────────────────────
st.set_page_config(page_title="全體時段分析（Global）", layout="wide")

DB_PATH = Path(__file__).parent / "data_3_2.db"

# ─────────────────────────────
# 工具：時間排序（12:00 → 23:45 → 00:00 → 11:45）
# ─────────────────────────────
def time_sort_key(t):
    hour, minute = map(int, t.split(":"))
    return hour * 60 + minute + (1440 if hour < 12 else 0)


# ─────────────────────────────
# 讀取全體時段資料
# ─────────────────────────────
@st.cache_data
def load_global_time_profile():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("""
            SELECT
                time,
                yt_sum,
                yt_weighted_avg,
                yt_weighted_diff,
                tw_sum,
                tw_weighted_avg,
                tw_weighted_diff
            FROM time_global_profile
        """, conn)
    return df


# ─────────────────────────────
# UI
# ─────────────────────────────
st.title("📊 全體時段表現分析（Global Time Profile）")

global_df = load_global_time_profile()

if global_df.empty:
    st.warning("time_global_profile 沒有資料")
    st.stop()

# None → 0
global_df = global_df.fillna(0)

# 取出 hour
global_df["hour"] = global_df["time"].apply(
    lambda t: int(t.split(":")[0])
)

# 只保留 18:00 ~ 05:00
global_df = global_df[
    (global_df["hour"] >= 18) |
    (global_df["hour"] < 5)
]

if global_df.empty:
    st.warning("18:00 ~ 05:00 區間內沒有資料")
    st.stop()

# 時間排序（中午邏輯仍適用）
global_df["sort_key"] = global_df["time"].apply(time_sort_key)
global_df.sort_values("sort_key", inplace=True)


# 建立有序 x 軸（避免 Streamlit 亂排）
global_df["time_ordered"] = pd.Categorical(
    global_df["time"],
    categories=global_df["time"],
    ordered=True
)

# ─────────────────────────────
# 圖 1：Live 數量（sum）
# ─────────────────────────────
st.markdown("## ⏱️ 全體 Live 數量（YT vs TW）")

st.line_chart(
    global_df,
    x="time_ordered",
    y=["yt_sum", "tw_sum"]
)

# ─────────────────────────────
# 圖 2：加權平均觀看數
# ─────────────────────────────
st.markdown("## 👀 全體加權平均觀看數（YT vs TW）")

st.line_chart(
    global_df,
    x="time_ordered",
    y=["yt_weighted_avg", "tw_weighted_avg"]
)

# ─────────────────────────────
# 圖 3：加權 diff
# ─────────────────────────────
st.markdown("## 📉 全體加權 Diff（YT vs TW）")

st.line_chart(
    global_df,
    x="time_ordered",
    y=["yt_weighted_diff", "tw_weighted_diff"]
)

# ─────────────────────────────
# 原始資料（可選）
# ─────────────────────────────
with st.expander("📄 查看原始資料"):
    st.dataframe(
        global_df.drop(columns=["sort_key"], errors="ignore"),
        width="stretch"
    )
