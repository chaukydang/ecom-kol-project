import pandas as pd
import numpy as np
from pathlib import Path

BRONZE = Path("data/lake/bronze")
SILVER = Path("data/lake/silver")
SILVER.mkdir(parents=True, exist_ok=True)

EV_B = BRONZE/"events"
IP_B = BRONZE/"item_props"

EV_S = SILVER/"events_clean"
PRICE_S = SILVER/"item_price_latest"
CAT_S = SILVER/"item_category_latest"
for d in [EV_S, PRICE_S, CAT_S]:
    d.mkdir(parents=True, exist_ok=True)

# 1) Events clean: drop nulls, keep necessary cols, ensure types
parts = sorted(EV_B.glob("date=*"))
for p in parts:
    date = p.name.split("=")[1]
    dfs = []
    for f in p.glob("*.parquet"):
        ch = pd.read_parquet(f)
        ch = ch.dropna(subset=["event","itemid"])
        ch["event"] = ch["event"].astype("string")
        ch["itemid"] = ch["itemid"].astype("string")
        ch["visitorid"] = ch["visitorid"].astype("string")
        dfs.append(ch[["visitorid","event","itemid"]])
    if not dfs: 
        continue
    day_df = pd.concat(dfs, ignore_index=True)
    day_df.to_parquet(EV_S/f"date={date}.parquet", index=False)

# 2) Item properties → latest price/category per item per day
def parse_price(x):
    try:
        sx = str(x).replace(",","").strip()
        return float(sx)
    except:
        return np.nan

ip_parts = sorted(IP_B.glob("date=*"))
for p in ip_parts:
    date = p.name.split("=")[1]
    dfs = []
    for f in p.glob("*.parquet"):
        ch = pd.read_parquet(f)
        ch["property"] = ch["property"].astype("string")
        dfs.append(ch[["itemid","property","value"]])
    if not dfs: 
        continue
    df = pd.concat(dfs, ignore_index=True)

    # Latest within the day: file is already per-day, nên chỉ cần lấy bản ghi cuối theo itemid, property
    df["rn"] = df.groupby(["itemid","property"]).cumcount(ascending=True)
    # PRICE
    price = df[df["property"].str.lower()=="price"].copy()
    price["price"] = price["value"].map(parse_price)
    price = price.dropna(subset=["price"])
    price_day_latest = price.groupby("itemid").tail(1)[["itemid","price"]]
    if not price_day_latest.empty:
        price_day_latest.to_parquet(PRICE_S/f"date={date}.parquet", index=False)

    # CATEGORYID
    cat = df[df["property"].str.lower()=="categoryid"].copy()
    cat_day_latest = cat.groupby("itemid").tail(1)[["itemid","value"]].rename(columns={"value":"categoryid"})
    if not cat_day_latest.empty:
        cat_day_latest.to_parquet(CAT_S/f"date={date}.parquet", index=False)

print("✅ Silver built → events_clean, item_price_latest, item_category_latest")
