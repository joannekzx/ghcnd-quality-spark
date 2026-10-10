-- An independent version of NOAA's streak check: a sensor stuck on one reading.
-- Find runs of at least :min_run consecutive days where a station reports the identical value
-- (gaps-and-islands with window functions), one row per run, with how many of its days NOAA flagged.
-- TMAX / TMIN / TAVG only: long identical runs are normal for PRCP (dry spells) and SNWD.
with ordered as (
    select station_id, element, date, value, qflag,
           lag(value) over w as prev_value,
           lag(date)  over w as prev_date
    from daily
    where element in ('TMAX', 'TMIN', 'TAVG')
    window w as (partition by station_id, element order by date)
),
marked as (
    select *,
           sum(case when prev_value is null or prev_value <> value or datediff(date, prev_date) <> 1
                    then 1 else 0 end)
               over (partition by station_id, element order by date rows unbounded preceding) as run_id
    from ordered
),
runs as (
    select station_id, element, value / 10.0 as value_c,
           count(*)      as run_days,
           count(qflag)  as flagged_days,
           min(date)     as start_date,
           max(date)     as end_date
    from marked
    group by station_id, element, run_id, value
    having count(*) >= :min_run
)
select r.station_id, r.element, r.value_c, r.run_days, r.flagged_days, r.start_date, r.end_date,
       s.country_code, s.name
from runs r
left join stations s using (station_id)
order by r.run_days desc
