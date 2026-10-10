-- Candidate station changes (move, new instrument, unit or sensor problem): a step in a station's
-- annual mean maximum temperature between consecutive well-observed years.
-- Heuristic, not proof: year-to-year swings of a few degrees are rare for an annual mean at one
-- station, so large steps are worth a look. :min_days keeps only well-observed years;
-- :jump_c is the size of step (deg C) to report.
with annual as (
    select station_id, year, count(*) as days, avg(value_native) as mean_tmax
    from daily
    where element = 'TMAX' and qflag is null
    group by station_id, year
    having count(*) >= :min_days
),
stepped as (
    select station_id, year, mean_tmax,
           lag(mean_tmax) over (partition by station_id order by year) as prev_mean,
           lag(year)      over (partition by station_id order by year) as prev_year
    from annual
)
select station_id, prev_year, year,
       round(prev_mean, 2)             as prev_mean_tmax,
       round(mean_tmax, 2)             as mean_tmax,
       round(mean_tmax - prev_mean, 2) as step_c
from stepped
where prev_year = year - 1
  and abs(mean_tmax - prev_mean) >= :jump_c
order by abs(mean_tmax - prev_mean) desc
