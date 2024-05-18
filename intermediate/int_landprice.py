#%%
import sys
import os
import duckdb
import pandas as pd
from japanmap import get_data, pref_code, pref_points
from shapely.geometry import Polygon
import requests
from tqdm import tqdm

sys.path.append("../stg")

import stg_stations
# %%
stations = stg_stations.main()
# %%
qpqo = get_data()
chiba = Polygon(pref_points(qpqo)[pref_code("千葉県")-1])
# %%
stations = stations[stations.within(chiba)]
# %%
stations["lat"] = stations["geometry"].centroid.y
stations["lng"] = stations["geometry"].centroid.x
# %%
target = [
    '千原線',
    '千葉線',
    '本線',
    '常磐新線',
    '新京成線',
    '1号線',
    '2号線',
    '5号線東西線',
    '10号線新宿線',
    '外房線',
    '京葉線',
    '常磐線',
    '総武線',
    '内房線',
    '武蔵野線',
    '野田線',
    '東葉高速線',
    '北総線',
 ]

stations = stations[stations["N02_003"].isin(target)]
# %%
station_codes = stations["N02_005c"].drop_duplicates().tolist()
# %%
url = "https://www.reinfolib.mlit.go.jp/ex-api/external/XIT001?"
prices = []

for station in tqdm(station_codes):
    for year in [i for i in range(2006,2024)]:
        params = {
            "year":year,
            "station":station,
            "area": 12
        }

        headers = {
            'Ocp-Apim-Subscription-Key': os.environ.get("MLIT_APIKEY")
        }
        try:
            price = pd.DataFrame(requests.get(
                url,
                headers=headers,
                params=params
            ).json()["data"])

            price["year"] = year
            price["station"] = station

            prices.append(price)
        except:
            pass
# %%
prices = pd.concat(prices)
# %%
station_title = stations[["N02_005","N02_005c"]]
# %%
duckdb.sql(
    """
create or replace table landprice as
with t as (
    select
        *,
        concat(cast(year as varchar), station) as uid
    from prices
    where year >= 2008
),
s as (
    select
        N02_005c as station,
        N02_005 as station_title
    from station_title
),
pre_e as (
    select
        uid,
        year,
        count(*) as cnt
    from t
    group by uid, year

),
ff as (
    select
        year,
        cnt,
        count(*) as frequency
    from
        pre_e
    group by
        year, cnt
),
mfy as (
    select
        year,
        cnt,
        frequency,
        rank() over (partition by year order by frequency desc) as rank
    from ff
),
e as (
select
    distinct
    uid
from pre_e
inner join (
    select
        year,
        cnt
    from
        mfy
    where
        rank = 1
    ) m on pre_e.year = m.year and pre_e.cnt = m.cnt
),
ft as (
    select
        year,
        station_title,
        round(avg(cast(PricePerUnit as int64))) as price,
        count(*) as count
    from t
    inner join s on t.station = s.station
    where uid not in (
        select
            uid
        from e
    )
    and Type = '宅地(土地)'
    and Region = '住宅地'
    and PricePerUnit != ''
    group by year, t.station, station_title
)

select
    year,
    station,
    ft.station_title,
    price,
    concat(cast(year as varchar), station) as uid,
    count
from ft
left join s on ft.station_title = s.station_title;

copy landprice to './outputs/int_landprice.parquet' (format parquet);
    """
)
# %%
prices.to_parquet("prices.parquet")
# %%
