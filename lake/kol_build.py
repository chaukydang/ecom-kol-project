import pandas as pd, numpy as np
from pathlib import Path

GOLD = Path("data/lake/gold")
OUT = GOLD/"kol_performance"
OUT.mkdir(parents=True, exist_ok=True)

daily = pd.read_parquet(GOLD/"daily_metrics"/"full.parquet").sort_values("date")

# tạo 12 creators giả lập (tier, followers, engagement)
rng = np.random.default_rng(42)
tiers = ["Nano","Micro","Macro","Mega"]
cats  = ["Beauty","Fashion","Tech","Lifestyle","Gaming","Food","Fitness"]
n = 12
followers_by_tier = {"Nano":(5_000,20_000),"Micro":(20_001,150_000),"Macro":(150_001,800_000),"Mega":(800_001,3_000_000)}
er_by_tier = {"Nano":(0.035,0.08),"Micro":(0.02,0.05),"Macro":(0.012,0.03),"Mega":(0.005,0.015)}

rows=[]
for i in range(1,n+1):
    tier = rng.choice(tiers, p=[0.25,0.4,0.25,0.10])
    fmin,fmax = followers_by_tier[tier]
    followers = int(rng.uniform(fmin,fmax))
    er = float(rng.uniform(*er_by_tier[tier]))
    cat = rng.choice(cats)
    rows.append({"creator_id":f"C{i:03d}","tier":tier,"followers":followers,"engagement_rate":round(er,4),"category":cat})
cr = pd.DataFrame(rows)
cr["w"] = cr["followers"]*(1+cr["engagement_rate"]*10)
cr["share"] = cr["w"]/cr["w"].sum()

# phân bổ theo ngày
out=[]
for _,d in daily.iterrows():
    views, atc, tx, rev = int(d["view"]), int(d["addtocart"]), int(d["transaction"]), float(d["revenue"])
    noise = rng.normal(1.0, 0.05, size=len(cr))
    shares = (cr["share"].values*noise).clip(min=0)
    shares = shares/shares.sum()
    alloc_views = np.floor(shares*views).astype(int)
    diff = views - alloc_views.sum()
    if diff>0: alloc_views[:diff]+=1
    for i,crow in cr.iterrows():
        v = int(alloc_views[i]); frac = v/views if views>0 else 0.0
        a = int(np.floor(frac*atc)); t = int(np.floor(frac*tx)); r = float(frac*rev)
        week = pd.to_datetime(d["date"]).strftime("W%V")
        out.append({"date":pd.to_datetime(d["date"]),
                    "creator_id":crow["creator_id"],"campaign_id":week,
                    "views":v,"addtocart":a,"transactions":t,"revenue":r,
                    "tier":crow["tier"],"engagement_rate":crow["engagement_rate"],"category":crow["category"]})
kol = pd.DataFrame(out)
kol["ctr_view_to_cart"]=np.where(kol["views"]>0, kol["addtocart"]/kol["views"], np.nan)
kol["conv_view_to_tx"]=np.where(kol["views"]>0, kol["transactions"]/kol["views"], np.nan)
kol.to_parquet(OUT/"full.parquet", index=False)
print("✅ Gold → kol_performance/full.parquet")
