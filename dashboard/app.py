# dashboard/app.py
import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
import os, io, requests  # <- thêm 3 import này

# ====== GITHUB LOADER (đọc từ branch `data`) ======
def _load_parquet_from_github(user: str, repo: str, branch: str, path_in_branch: str) -> pd.DataFrame:
    url = f"https://raw.githubusercontent.com/{user}/{repo}/{branch}/{path_in_branch}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    bio = io.BytesIO(r.content)
    return pd.read_parquet(bio)

def load_data_with_fallback() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Ưu tiên đọc GitHub (nhánh `data`), nếu không có thì fallback về local.
    Trả về: (daily_df, kol_df, "github" | "local")
    """
    user   = os.getenv("GITHUB_USER")
    repo   = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_DATA_BRANCH", "data")

    # thử GitHub trước nếu có cấu hình env
    if user and repo:
        try:
            d = _load_parquet_from_github(user, repo, branch, "daily_metrics/full.parquet")
            k = _load_parquet_from_github(user, repo, branch, "kol_performance/full.parquet")
            # chuẩn hoá datetime
            if "date" in d.columns: d["date"] = pd.to_datetime(d["date"])
            if "date" in k.columns: k["date"] = pd.to_datetime(k["date"])
            return d.sort_values("date"), k.sort_values("date"), "github"
        except Exception as e:
            st.warning(f"Load from GitHub failed ({e}). Falling back to local files…")

    # fallback local
    DAILY_PATH = Path("data/lake/gold/daily_metrics/full.parquet")
    KOL_PATH   = Path("data/lake/gold/kol_performance/full.parquet")
    if DAILY_PATH.exists() and KOL_PATH.exists():
        d = pd.read_parquet(DAILY_PATH);  k = pd.read_parquet(KOL_PATH)
        if "date" in d.columns: d["date"] = pd.to_datetime(d["date"])
        if "date" in k.columns: k["date"] = pd.to_datetime(k["date"])
        return d.sort_values("date"), k.sort_values("date"), "local"

    st.error("❌ No data found. Either configure GitHub env (GITHUB_USER/REPO/BRANCH) or generate local Gold parquet via pipeline.")
    st.stop()

# ============== PAGE CONFIG ==============
st.set_page_config(
    page_title="E-comm & KOL Performance",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============== CONSTANTS ==============
DAILY_PATH = Path("data/lake/gold/daily_metrics/full.parquet")
KOL_PATH   = Path("data/lake/gold/kol_performance/full.parquet")

# ============== STYLES (soft cards) ==============
st.markdown("""
<style>
/* ---------- KPI CARDS: chiều cao bằng nhau ---------- */
.kpi{
  border: 1px solid #eaeaea;
  border-radius: 14px;
  padding: 16px 18px;
  background: #ffffff;
  box-shadow: 0 1px 6px rgba(0,0,0,0.06);

  /* QUAN TRỌNG để đều nhau */
  height: 150px;                       /* chỉnh 140–160 tuỳ font bạn */
  display: flex;
  flex-direction: column;
  justify-content: space-between;      /* tiêu đề ở trên, sub ở dưới */
}

.kpi h4{
  margin: 0 0 6px 0;
  font-size: 0.92rem;
  color: #6b7280;
  font-weight: 700;
  line-height: 1.1;
}

.kpi .v{
  font-size: 1.6rem;
  font-weight: 800;
  color: #111827;
  line-height: 1.2;
}

.kpi .s{
  font-size: 0.86rem;
  color: #6b7280;
  margin-top: 8px;

  /* đảm bảo không làm card cao hơn do xuống nhiều dòng */
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  min-height: 1.2em;                   /* giữ hàng chữ ổn định */
}
</style>
""", unsafe_allow_html=True)

# ============== HELPERS ==============
@st.cache_data(ttl=600)
def load_df(path: Path):
    df = pd.read_parquet(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df

def kpi(col, title, value, sub=None, fmt="int"):
    # Chuẩn hoá hiển thị giá trị rỗng/NaN
    if value is None or (isinstance(value, float) and np.isnan(value)):
        safe_val = "—"
    else:
        safe_val = value

    if fmt == "int":
        try:
            val = f"{int(float(safe_val)):,}"
        except Exception:
            val = "—"
    elif fmt == "usd":
        try:
            val = f"${float(safe_val):,.0f}"
        except Exception:
            val = "—"
    elif fmt == "pct":
        try:
            val = f"{float(safe_val)*100:.2f}%"
        except Exception:
            val = "—"
    elif fmt == "text":
        val = str(safe_val)
    else:
        val = str(safe_val)

    with col:
        st.markdown(f"""
        <div class="kpi">
          <h4>{title}</h4>
          <div class="v">{val}</div>
          {f'<div class="s">{sub}</div>' if sub else ''}
        </div>
        """, unsafe_allow_html=True)

def quick_range_range(full_min, full_max, choice):
    if choice == "Last 7 days":
        return full_max - timedelta(days=6), full_max
    if choice == "Last 30 days":
        return full_max - timedelta(days=29), full_max
    if choice == "MTD":
        return full_max.replace(day=1), full_max
    if choice == "YTD":
        return full_max.replace(month=1, day=1), full_max
    return full_min, full_max

# ============== LOAD DATA (GitHub-first with local fallback) ==============
daily, kol, source = load_data_with_fallback()
st.caption(f"Data source: **{source}**")

# ============== SIDEBAR: FILTERS & HELP ==============
with st.sidebar:
    st.subheader("Filters")
    min_d, max_d = daily["date"].min().date(), daily["date"].max().date()

    preset = st.radio("Quick range", ["All time", "Last 7 days", "Last 30 days", "MTD", "YTD"], horizontal=False)
    if preset == "All time":
        dfrom, dto = min_d, max_d
    else:
        a,b = quick_range_range(min_d, max_d, preset)
        dfrom, dto = a.date(), b.date()

    dfrom, dto = st.date_input("Custom range", value=(dfrom, dto), min_value=min_d, max_value=max_d)

    tiers = st.multiselect("Tier", sorted([x for x in kol["tier"].dropna().unique()]))
    cats  = st.multiselect("Category", sorted([x for x in kol["category"].dropna().unique()]))
    creators = st.multiselect("Creator", sorted([x for x in kol["creator_id"].dropna().unique()]))

    st.markdown("---")
    with st.expander("Hướng dẫn nhanh"):
        st.write("""
        - **Views, ATC, Transactions, Revenue**: tổng trong khoảng ngày.
        - **CTR (view→ATC)** = ATC / Views, **CVR (view→tx)** = Transactions / Views.
        - **AOV** ≈ Revenue / Transactions (Revenue là *proxy* theo `price`).
        - **Creators tab**: so sánh KOL theo tier/category, có **Profile** drill-down.
        - **Ops tab**: mô phỏng tăng trưởng, upload cost để xem **ROAS**.
        """)

# apply filters
mask_d = (daily["date"].dt.date >= dfrom) & (daily["date"].dt.date <= dto)
fdaily = daily.loc[mask_d].copy()

mask_k = (kol["date"].dt.date >= dfrom) & (kol["date"].dt.date <= dto)
fkol = kol.loc[mask_k].copy()
if tiers:    fkol = fkol[fkol["tier"].isin(tiers)]
if cats:     fkol = fkol[fkol["category"].isin(cats)]
if creators: fkol = fkol[fkol["creator_id"].isin(creators)]

# ============== HEADER ==============
st.title("E-commerce & KOL Performance")
# st.caption("Bronze → Silver → Gold • Dashboard thân thiện cho Marketing & Growth")

# ============== TABS ==============
st.markdown("""
<style>
/* ---------- BOLD TABS (bao phủ nhiều phiên bản Streamlit) ---------- */
/* Nút tab (Streamlit gần đây dùng <button role="tab">) */
.stTabs button[role="tab"] {
  font-weight: 800 !important;         /* IN ĐẬM */
  color: #0f172a !important;
  letter-spacing: .1px;
}

/* Label tab đôi khi là <p> hoặc <span> bên trong button */
.stTabs button[role="tab"] p,
.stTabs button[role="tab"] span,
.stTabs button[role="tab"] div {
  font-weight: 800 !important;         /* IN ĐẬM */
  font-size: 0.98rem !important;
}

/* Tab đang ACTIVE: đổi màu + đậm hơn */
.stTabs button[role="tab"][aria-selected="true"] {
  color: #1d4ed8 !important;           /* xanh primary */
  font-weight: 900 !important;
  border-bottom: 3px solid #1d4ed8 !important; /* underline dày hơn */
}

/* Một số bản cũ: <div role="tab"> thay vì button */
.stTabs [role="tab"] {
  font-weight: 800 !important;
}
.stTabs [role="tab"][aria-selected="true"] {
  color: #1d4ed8 !important;
  font-weight: 900 !important;
  border-bottom: 3px solid #1d4ed8 !important;
}
</style>
""", unsafe_allow_html=True)


tab1, tab2, tab3 = st.tabs(["Overview", "Creators", "Ops (Simulation & ROAS)"])

# -------------------- TAB 1: OVERVIEW --------------------

st.markdown("""
<style>
.section-title {
  margin: 8px 0 12px 0;
  font-weight: 800;    /* in đậm */
  font-size: 1.2rem;   /* chữ to hơn 1 chút */
  color: #111827;
  border-left: 4px solid #2563eb;  /* thêm thanh xanh để nổi bật */
  padding-left: 8px;
}
</style>
""", unsafe_allow_html=True)

with tab1:
    st.markdown('<div class="section-title">Key Metrics</div>', unsafe_allow_html=True)
    v = int(fdaily["view"].sum())
    a = int(fdaily["addtocart"].sum())
    t = int(fdaily["transaction"].sum())
    r = float(fdaily["revenue"].sum())
    ctr = (a / v) if v>0 else 0.0
    cvr = (t / v) if v>0 else 0.0
    aov = (r / t) if t>0 else 0.0

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    kpi(c1, "Views", v, "View sessions", "int")
    kpi(c2, "Add-to-Cart", a, "Added to cart", "int")
    kpi(c3, "Transactions", t, "Orders", "int")
    kpi(c4, "Revenue (proxy)", r, "Total item prices in transactions", "usd")
    kpi(c5, "CTR (view→ATC)", ctr, "ATC / Views", "pct")
    kpi(c6, "CVR (view→tx)", cvr, "Tx / Views", "pct")

    st.markdown('<div class="section-title">Funnel by day</div>', unsafe_allow_html=True)
    ts = fdaily[["date","view","addtocart","transaction"]].set_index("date")
    fig_funnel = go.Figure()
    fig_funnel.add_trace(go.Scatter(x=ts.index, y=ts["view"], name="Views", mode="lines"))
    fig_funnel.add_trace(go.Scatter(x=ts.index, y=ts["addtocart"], name="Add-to-Cart", mode="lines"))
    fig_funnel.add_trace(go.Scatter(x=ts.index, y=ts["transaction"], name="Transactions", mode="lines"))
    st.plotly_chart(fig_funnel, use_container_width=True)

    st.markdown('<div class="section-title">Revenue trend</div>', unsafe_allow_html=True)
    st.plotly_chart(px.line(fdaily, x="date", y="revenue", labels={"revenue":"Revenue"}), use_container_width=True)

    st.markdown('<div class="section-title">Weekday pattern</div>', unsafe_allow_html=True)
    tmp = fdaily.copy()
    tmp["weekday"] = tmp["date"].dt.day_name()
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    tmp["weekday"] = pd.Categorical(tmp["weekday"], categories=order, ordered=True)
    bar = tmp.groupby("weekday")[["view","transaction","revenue"]].sum().reset_index()
    st.plotly_chart(px.bar(bar, x="weekday", y=["view","transaction","revenue"], barmode="group"), use_container_width=True)

    with st.expander("Tải dữ liệu (Overview)"):
        st.download_button("Download daily (filtered)", fdaily.to_csv(index=False).encode("utf-8"),
                           file_name="daily_filtered.csv", use_container_width=True)

# -------------------- TAB 2: CREATORS --------------------
with tab2:
    left, right = st.columns([2.6, 1.4])
    with left:
        st.markdown('<div class="section-title">Top creators (by revenue)</div>', unsafe_allow_html=True)
        top = (fkol.groupby(["creator_id","tier","category"], observed=False)
          .agg(views=("views","sum"),
               atc=("addtocart","sum"),
               tx=("transactions","sum"),
               revenue=("revenue","sum"))
          .reset_index())

        top["CTR"] = np.where(top["views"]>0, top["atc"]/top["views"], np.nan)
        top["CVR"] = np.where(top["views"]>0, top["tx"]/top["views"], np.nan)
        top = top.sort_values("revenue", ascending=False)

        st.dataframe(top, use_container_width=True, height=360)
        st.download_button("Download creators (filtered)", top.to_csv(index=False).encode("utf-8"),
                           file_name="creators_filtered.csv", use_container_width=True)

    with right:
        st.markdown('<div class="section-title">Top 20 bar</div>', unsafe_allow_html=True)
        st.plotly_chart(
            px.bar(top.head(20), x="creator_id", y="revenue", color="tier",
                   title=None), use_container_width=True, height=300
        )

        st.markdown('<div class="section-title">Bubble: views → tx</div>', unsafe_allow_html=True)
        st.plotly_chart(
            px.scatter(top, x="views", y="tx", size="revenue", color="tier",
                       hover_data=["creator_id","category"]), use_container_width=True, height=300
        )

    st.markdown("---")
    st.markdown('<div class="section-title">Creator profile</div>', unsafe_allow_html=True)
    cids = sorted(fkol["creator_id"].unique().tolist())
    if len(cids) == 0:
        st.info("Không có dữ liệu theo filter hiện tại.")
    else:
        sel = st.selectbox("Choose creator", cids, index=0)
        cdf = fkol[fkol["creator_id"]==sel].copy()
        meta = cdf[["tier","category"]].drop_duplicates().head(1)
        tier = meta["tier"].iat[0] if len(meta)>0 else "-"
        cat  = meta["category"].iat[0] if len(meta)>0 else "-"

        # KPI của creator vs. tổng & benchmark theo tier
        total = fkol.agg({"views":"sum","transactions":"sum","revenue":"sum","addtocart":"sum"})
        mine  = cdf.agg({"views":"sum","transactions":"sum","revenue":"sum","addtocart":"sum"})
        share_traffic = (mine["views"]/max(total["views"],1))
        share_rev     = (mine["revenue"]/max(total["revenue"],1))
        cvr  = mine["transactions"]/max(mine["views"],1)
        ctr  = mine["addtocart"]/max(mine["views"],1)
        aov  = mine["revenue"]/max(mine["transactions"],1)

        tdf = fkol[fkol["tier"]==tier]
        t_agg = tdf.groupby("creator_id").agg(v=("views","sum"), tx=("transactions","sum"), r=("revenue","sum"), a=("addtocart","sum"))
        t_cvr = (t_agg["tx"]/t_agg["v"]).mean() if len(t_agg)>0 else np.nan
        t_ctr = (t_agg["a"]/t_agg["v"]).mean() if len(t_agg)>0 else np.nan
        t_aov = (t_agg["r"]/t_agg["tx"]).mean() if (len(t_agg)>0 and (t_agg["tx"]>0).any()) else np.nan

        c1,c2,c3,c4,c5,c6 = st.columns(6)
        kpi(c1, f"{sel}", sel, f"{tier} • {cat}", fmt="text")
        kpi(c2, "Traffic share", share_traffic, "Views share", "pct")
        kpi(c3, "Revenue share", share_rev, "Contribution", "pct")
        kpi(c4, "AOV", aov, "Revenue / Tx", "usd")
        kpi(c5, "CVR (view→tx)", cvr, f"vs tier { (cvr - (t_cvr or 0))*100:+.1f} pp", "pct")
        kpi(c6, "CTR (view→ATC)", ctr, f"vs tier { (ctr - (t_ctr or 0))*100:+.1f} pp", "pct")

        cdf_day = cdf.groupby("date")[["views","addtocart","transactions","revenue"]].sum().reset_index()
        colA, colB = st.columns(2)
        colA.plotly_chart(px.line(cdf_day, x="date", y=["views","addtocart","transactions"], title="Funnel"), use_container_width=True)
        colB.plotly_chart(px.line(cdf_day, x="date", y="revenue", title="Revenue"), use_container_width=True)

        wk = cdf.groupby("campaign_id").agg(views=("views","sum"), atc=("addtocart","sum"),
                                            tx=("transactions","sum"), revenue=("revenue","sum")).reset_index()
        wk["CVR"] = np.where(wk["views"]>0, wk["tx"]/wk["views"], np.nan)
        st.dataframe(wk.sort_values("revenue", ascending=False), use_container_width=True)
        st.download_button("⬇️ Download profile by week", wk.to_csv(index=False).encode("utf-8"),
                           file_name=f"{sel}_by_week.csv", use_container_width=True)

# -------------------- TAB 3: OPS --------------------
with tab3:
    st.markdown('<div class="section-title">Simulation (growth scenarios)</div>', unsafe_allow_html=True)
    col1,col2,col3 = st.columns(3)
    with col1:
        traffic = st.slider("Traffic lift (%)", 0, 200, 15, step=5)
    with col2:
        conv    = st.slider("Conversion lift (%)", 0, 100, 5, step=5)
    with col3:
        scope   = st.selectbox("Apply to", ["All filtered creators", "Top 3 by revenue"])

    base = fkol.copy()
    base["cvr"] = np.where(base["views"]>0, base["transactions"]/base["views"], 0.0)

    if scope.startswith("Top"):
        top3 = (base.groupby("creator_id")["revenue"].sum().sort_values(ascending=False).head(3).index.tolist())
        mask = base["creator_id"].isin(top3)
    else:
        mask = np.ones(len(base), dtype=bool)

    base["views_sim"] = base["views"]
    base.loc[mask,"views_sim"] *= (1 + traffic/100)
    base["cvr_sim"] = base["cvr"]
    base.loc[mask,"cvr_sim"] *= (1 + conv/100)
    base["tx_sim"] = base["views_sim"] * base["cvr_sim"]

    arp_tx = (base["revenue"].sum() / max(base["transactions"].sum(), 1))
    base["rev_sim"] = base["tx_sim"] * arp_tx

    agg = (base.groupby("date")[["revenue"]].sum().rename(columns={"revenue":"rev_base"})
                 .assign(rev_sim = base.groupby("date")["rev_sim"].sum().values))
    st.plotly_chart(px.line(agg, x=agg.index, y=["rev_base","rev_sim"], title="Revenue: base vs simulated"),
                    use_container_width=True)
    delta = base["rev_sim"].sum() - base["revenue"].sum()
    st.metric("Projected Revenue (period)", f"${delta:,.0f}")

    st.markdown("---")
    st.markdown('<div class="section-title">ROAS (upload cost)</div>', unsafe_allow_html=True)
    st.caption("CSV format đơn giản: `creator_id,cost` (hoặc `creator_id,campaign_id,cost`).")
    up = st.file_uploader("Upload creator_costs.csv", type=["csv"])
    if up is not None:
        costs = pd.read_csv(up)
        costs.columns = [c.lower() for c in costs.columns]
        if "campaign_id" in costs.columns:
            merged = fkol.merge(costs, on=["creator_id","campaign_id"], how="left")
        else:
            merged = fkol.merge(costs, on=["creator_id"], how="left")
        merged["cost"] = merged["cost"].fillna(0.0)
        roas = (merged.groupby(["creator_id","campaign_id"])
                        .agg(revenue=("revenue","sum"), cost=("cost","sum"))
                        .reset_index())
        roas["ROAS"] = np.where(roas["cost"]>0, roas["revenue"]/roas["cost"], np.nan)
        st.dataframe(roas.sort_values(["ROAS","revenue"], ascending=[False,False]), use_container_width=True)
        st.download_button("Download ROAS", roas.to_csv(index=False).encode("utf-8"),
                           file_name="roas_filtered.csv", use_container_width=True)

# ============== FOOTER ==============
st.caption("© KOL & Growth Analytics — Clean, simple, and stakeholder-friendly.")
