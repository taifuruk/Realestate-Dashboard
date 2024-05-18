#%%
import duckdb
#%%
duckdb.sql(
    """
create or replace table d_vs_p as
select
    line_title,
    operator,
    d.station_title,
    distances,
    lat,
    lng,
    year,
    price,
    count
from '../intermediate/outputs/int_duration.parquet' d
inner join '../intermediate/outputs/int_landprice.parquet' l on d.station = l.station;
copy d_vs_p to './outputs/distances_vs_prices.parquet' (format parquet);
    """
)
# %%
