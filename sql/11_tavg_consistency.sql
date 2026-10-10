-- Internal consistency across elements: a daily mean (TAVG) outside that day's [TMIN, TMAX] range.
-- Only days that report all three. :tol allows rounding (deg C).
with t as (
    select station_id, date,
           max(case when element = 'TMAX' then value_native end) as tmax,
           max(case when element = 'TMIN' then value_native end) as tmin,
           max(case when element = 'TAVG' then value_native end) as tavg,
           max(case when element = 'TAVG' then qflag end)        as tavg_flag,
           max(case when element = 'TMAX' then qflag end)        as tmax_flag,
           max(case when element = 'TMIN' then qflag end)        as tmin_flag
    from daily
    where element in ('TMAX', 'TMIN', 'TAVG')
    group by station_id, date
    having count(*) = 3
)
select count(*)                                                                         as days_with_all_three,
       count_if(tavg < tmin - :tol or tavg > tmax + :tol)                               as tavg_outside_range,
       count_if((tavg < tmin - :tol or tavg > tmax + :tol)
                and tavg_flag is null and tmax_flag is null and tmin_flag is null)      as outside_and_nothing_flagged
from t
where tmax >= tmin
