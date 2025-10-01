import zipfile, os
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

RAW_ZIP = Path("data/raw_master/Retailrocket Recommender Dataset.zip")
LAKE = Path("data/lake/bronze")
EV_OUT = LAKE/"events"
IP_OUT = LAKE/"item_props"
META_OUT = Path("data/lake")  # để category_tree.csv

EV_OUT.mkdir(parents=True, exist_ok=True)
IP_OUT.mkdir(parents=True, exist_ok=True)
META_OUT.mkdir(parents=True, exist_ok=True)

assert RAW_ZIP.exists(), f"Không thấy ZIP ở {RAW_ZIP}. Hãy đặt file vào data/raw/"

def epoch_to_date(s):
    s = pd.to_numeric(s, errors="coerce")
    ms = s > 10**11
    dt = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if ms.any():     dt.loc[ms] = pd.to_datetime(s.loc[ms], unit="ms", errors="coerce")
    if (~ms).any():  dt.loc[~ms] = pd.to_datetime(s.loc[~ms], unit="s", errors="coerce")
    return dt.dt.date

with zipfile.ZipFile(RAW_ZIP) as zf:
    names = zf.namelist()
    ev_name = next(n for n in names if n.endswith("events.csv"))
    ip1 = next(n for n in names if "item_properties_part1" in n and n.endswith(".csv"))
    ip2 = next(n for n in names if "item_properties_part2" in n and n.endswith(".csv"))
    ct = next(n for n in names if n.endswith("category_tree.csv"))

    # 1) EVENTS → partition per day
    with zf.open(ev_name) as f:
        for i, chunk in enumerate(pd.read_csv(f, chunksize=1_000_000, usecols=["timestamp","visitorid","event","itemid"])):
            chunk["date"] = epoch_to_date(chunk["timestamp"]).astype("string")
            for day, df_day in chunk.groupby("date"):
                if pd.isna(day) or day == "NaT": 
                    continue
                out_dir = EV_OUT/f"date={day}"
                out_dir.mkdir(parents=True, exist_ok=True)
                df_day.to_parquet(out_dir/f"part-{i:05d}.parquet", index=False)

    # 2) ITEM PROPERTIES (giữ lại tất cả property; partition theo ngày)
    for ip_name in [ip1, ip2]:
        with zf.open(ip_name) as f:
            for j, chunk in enumerate(pd.read_csv(f, chunksize=1_000_000, usecols=["timestamp","itemid","property","value"])):
                chunk["date"] = epoch_to_date(chunk["timestamp"]).astype("string")
                for day, df_day in chunk.groupby("date"):
                    if pd.isna(day) or day == "NaT": 
                        continue
                    out_dir = IP_OUT/f"date={day}"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    df_day.to_parquet(out_dir/f"part-{j:05d}.parquet", index=False)

    # 3) Category tree (giữ file gốc)
    with zf.open(ct) as f:
        cat = pd.read_csv(f)
    cat.to_parquet(META_OUT/"category_tree.parquet", index=False)

print("✅ Ingest done → data/lake/bronze/")
