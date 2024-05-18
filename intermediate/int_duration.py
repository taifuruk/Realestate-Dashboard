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
stations = stations[["N02_003","N02_004","N02_005","N02_005c","lat","lng"]]
# %%
duckdb.sql(
    """
create or replace table d as
select
    N02_003 as line_title,
    N02_004 as operator,
    N02_005 as station_title,
    N02_005c as station,
    6371000 * 2 * asin(sqrt(
        power(sin((radians(lat) - radians(lat_tokyo)) / 2), 2) +
        cos(radians(lat_tokyo)) * cos(radians(lat)) *
        power(sin((radians(lng) - radians(lng_tokyo)) / 2), 2)
    )) / 1000 as distances,
    lat,
    lng
from stations
cross join (
    select
        35.6813956596396 as lat_tokyo,
        139.76715526960268 as lng_tokyo
);

copy d to './outputs/int_duration.parquet' (format parquet);
    """
)
# %%
