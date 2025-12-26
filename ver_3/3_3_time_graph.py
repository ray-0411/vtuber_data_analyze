import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

# ─────────────────────────────
# 基本設定
# ─────────────────────────────
st.set_page_config(page_title="YT / TW 時段分析", layout="wide")

DB_PATH = Path(__file__).parent / "data_3_2.db"

#streamlit run ver_3/3_3_time_graph.py

# ─────────────────────────────
# 工具：時間排序
# ─────────────────────────────
def time_sort_key(t):
    hour = int(t.split(":")[0])
    return hour if hour >= 12 else hour + 24


# ─────────────────────────────
# 讀頻道清單
# ─────────────────────────────
@st.cache_data
def load_channels():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("""
            SELECT DISTINCT channel_id, channel_name
            FROM yt_time_profile
            UNION
            SELECT DISTINCT channel_id, channel_name
            FROM tw_time_profile
            ORDER BY channel_name;
        """, conn)
    return df



@st.cache_data
def load_time_profile(table, channel_id):
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(f"""
            SELECT
                time,
                live_count,
                avg_viewers
            FROM {table}
            WHERE channel_id = ?
        """, conn, params=(channel_id,))
    return df


# ─────────────────────────────
# UI
# ─────────────────────────────
st.title("📊 YT / TW 時段表現分析")

channels = load_channels()

channel_name = st.selectbox(
    "選擇實況主",
    channels["channel_name"]
)

channel_id = channels.loc[
    channels["channel_name"] == channel_name,
    "channel_id"
].iloc[0]

# ─────────────────────────────
# 載入資料
# ─────────────────────────────
yt_df = load_time_profile("yt_time_profile", channel_id)
tw_df = load_time_profile("tw_time_profile", channel_id)

st.subheader(f"🎮 {channel_name}")

# ─────────────────────────────
# 整理 YT / TW 資料（疊線用）
# ─────────────────────────────
def prepare_df(df, prefix):
    if df.empty:
        return pd.DataFrame(columns=["time", "sort_key",
                                     f"{prefix}_live",
                                     f"{prefix}_avg"])

    df = df.copy()
    df["sort_key"] = df["time"].apply(time_sort_key)

    df = df[["time", "sort_key", "live_count", "avg_viewers"]]
    df.rename(columns={
        "live_count": f"{prefix}_live",
        "avg_viewers": f"{prefix}_avg"
    }, inplace=True)

    return df


yt_p = prepare_df(yt_df, "yt")
tw_p = prepare_df(tw_df, "tw")

# outer join：任一邊有資料就顯示
merged = pd.merge(
    yt_p,
    tw_p,
    on=["time", "sort_key"],
    how="outer"
)

if merged.empty:
    st.warning("沒有 YT / TW 時段資料")
    st.stop()

# 依你要的順序排序時間
merged.sort_values("sort_key", inplace=True)

# 建立「有序的時間軸」
merged["time_ordered"] = pd.Categorical(
    merged["time"],
    categories=merged["time"],
    ordered=True
)

# ─────────────────────────────
# 畫圖：Live 次數（疊線）
# ─────────────────────────────
st.markdown("## ⏱️ Live 次數（YT vs TW）")

st.line_chart(
    merged,
    x="time_ordered",
    y=[c for c in ["yt_live", "tw_live"] if c in merged.columns]
)

# ─────────────────────────────
# 畫圖：平均觀看數（疊線）
# ─────────────────────────────
st.markdown("## 👀 平均觀看數（YT vs TW）")

st.line_chart(
    merged,
    x="time_ordered",
    y=[c for c in ["yt_avg", "tw_avg"] if c in merged.columns]
)


# ─────────────────────────────
# 原始資料（可選）
# ─────────────────────────────
with st.expander("📄 查看原始資料"):
    
    st.dataframe(yt_df, width="stretch")
    st.dataframe(tw_df, width="stretch")

