import pandas as pd
import numpy as np
from pathlib import Path

SILVER = Path("data/lake/silver")
GOLD = Path("data/lake/gold")
(GOLD/"daily_metrics").mkdir(parents=True, exist_ok=True)
(GOLD/"top_items").mkdir(parents=True, exist_ok=True)

# 1) Load all events_clean (by day)
ev_files = sorted((SILVER/"events_clean").glob("date=*.parquet"))
ev = pd.concat([pd.read_parquet(f).assign(date=f.stem.split("=")[1]) for f in ev_files], ignore_index=True)
ev["date"] = pd.to_datetime(ev["date"])

# 2) Daily counts
daily = ev.groupby(["date","event"])["event"].count().unstack(fill_value=0).reset_index()
for col in ["view","addtocart","transaction"]:
    if col not in daily.columns: daily[col] = 0
daily["ctr_view_to_cart"] = np.where(daily["view"]>0, daily["addtocart"]/daily["view"], np.nan)
daily["conv_view_to_tx"] = np.where(daily["view"]>0, daily["transaction"]/daily["view"], np.nan)

# 3) Build latest price per item as-of each day (coarse: lấy "last known" overall nếu thiếu)
price_files = sorted((SILVER/"item_price_latest").glob("date=*.parquet"))
price_df = []
for f in price_files:
    d = f.stem.split("=")[1]
    tmp = pd.read_parquet(f).assign(date=pd.to_datetime(d))
    price_df.append(tmp)
price_df = pd.concat(price_df, ignore_index=True) if price_df else pd.DataFrame(columns=["itemid","price","date"])

# lấy latest price overall (fallback)
price_latest_overall = price_df.sort_values("date").groupby("itemid").tail(1).set_index("itemid")["price"].to_dict()
median_price = (pd.Series(price_latest_overall).median() if len(price_latest_overall)>0 else 100.0)

# 4) Revenue proxy: join transactions with price (fallback to latest overall or median)
tx = ev[ev["event"]=="transaction"].copy()
if not tx.empty:
    # attach price: first try by exact day (merge last per item up to day is expensive; dùng overall latest fallback)
    tx["itemid_num"] = pd.to_numeric(tx["itemid"], errors="coerce").astype("Int64")
    tx["price"] = tx["itemid"].map(price_latest_overall).astype(float)
    tx["price"] = tx["price"].fillna(median_price)
    rev = tx.groupby("date")["price"].sum().reset_index().rename(columns={"price":"revenue"})
else:
    rev = pd.DataFrame({"date": daily["date"], "revenue": 0.0})

daily = daily.merge(rev, on="date", how="left").fillna({"revenue":0.0})
daily.to_parquet(GOLD/"daily_metrics"/"full.parquet", index=False)

# 5) Top items
top = tx["itemid_num"].value_counts().rename_axis("itemid").reset_index(name="transactions") if not tx.empty else pd.DataFrame(columns=["itemid","transactions"])
if not top.empty:
    top["price"] = top["itemid"].map(price_latest_overall)
top.to_parquet(GOLD/"top_items"/"top.parquet", index=False)

print("✅ Gold built → daily_metrics/full.parquet, top_items/top.parquet")
