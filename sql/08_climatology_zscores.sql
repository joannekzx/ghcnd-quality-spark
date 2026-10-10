-- An independent detector: how far is each daily TMAX / TMIN from what that station normally records
-- in that week of the year? Robust z-score = (value - weekly median) / (1.4826 * MAD), where the median
-- and MAD (median absolute deviation) come from the station's own record for the same 7-day bin of the
-- year, across all years. The observation is included in its own climatology; with decades of data per
-- bin that is negligible, and bins with fewer than :min_obs observations are skipped.
-- :min_scale floors the scale (deg C) so a very steady station does not turn small wobbles into huge z.
-- This does NOT use NOAA's flags or NOAA's bounds, so it can disagree with them.
with obs as (
    select station_id, element, date, value_native as v, qflag,
           cast(floor((dayofyear(date) - 1) / 7) as int) as wk
    from daily
    where element in ('TMAX', 'TMIN')
),
clim as (
    select station_id, element, wk, percentile_approx(v, 0.5) as med
    from obs group by station_id, element, wk
    having count(*) >= :min_obs
),
dev as (
    select o.station_id, o.element, o.wk, abs(o.v - c.med) as ad
    from obs o join clim c using (station_id, element, wk)
),
mad as (
    select station_id, element, wk, percentile_approx(ad, 0.5) as mad
    from dev group by station_id, element, wk
)
select o.station_id, o.element, o.date, o.v as value_c, o.qflag, c.med as week_median_c,
       greatest(m.mad * 1.4826, cast(:min_scale as double)) as scale_c,
       (o.v - c.med) / greatest(m.mad * 1.4826, cast(:min_scale as double)) as z
from obs o
join clim c using (station_id, element, wk)
join mad  m using (station_id, element, wk)
