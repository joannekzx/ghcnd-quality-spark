-- How complete is a station's record within the years it exists?
-- For TMAX: every station-year strictly between a station's first and last observed year, including
-- years with no data at all, scored as observed days / days in the year. The first and last year are
-- left out on purpose: a station that opens in June or is still reporting today (the current year is
-- incomplete) would otherwise look incomplete for reasons that are not data quality.
with per_year as (
    select station_id, year, count(*) as observed_days
    from daily
    where element = 'TMAX'
    group by station_id, year
),
span as (
    select station_id, min(year) as first_year, max(year) as last_year
    from per_year
    group by station_id
),
station_years as (
    select s.station_id, y.year
    from span s
    lateral view explode(sequence(s.first_year + 1, s.last_year - 1)) y as year
    where s.last_year - s.first_year >= 2
),
scored as (
    select sy.station_id,
           sy.year,
           coalesce(p.observed_days, 0) as observed_days,
           coalesce(p.observed_days, 0) /
               (case when (sy.year % 4 = 0 and sy.year % 100 <> 0) or sy.year % 400 = 0 then 366.0 else 365.0 end)
               as completeness
    from station_years sy
    left join per_year p on p.station_id = sy.station_id and p.year = sy.year
)
select cast(floor(year / 10) * 10 as int)                               as decade,
       count(*)                                                         as station_years,
       round(avg(completeness), 3)                                      as mean_completeness,
       round(100.0 * avg(case when observed_days = 0 then 1 else 0 end), 1)   as pct_empty_years,
       round(100.0 * avg(case when completeness < 0.9 then 1 else 0 end), 1)  as pct_under_90
from scored
group by 1
order by 1
