-- Independent plausibility checks, compared with what NOAA already flagged.
-- For each rule: how many observations break it, and how many of those carry a NOAA quality flag.
-- Unflagged violations are the interesting number: bad values NOAA's checks let through.
--
-- Parameters are loose physical bounds (not statistics), passed in by the caller:
--   :tmax_max  deg C   hottest plausible daily maximum anywhere on Earth
--   :tmin_min  deg C   coldest plausible daily minimum anywhere on Earth
--   :prcp_max  mm      wettest plausible single day
with rules as (
    select 'TMAX above bound'     as rule, element, qflag from daily where element = 'TMAX' and value_native > :tmax_max
    union all
    select 'TMIN below bound'     as rule, element, qflag from daily where element = 'TMIN' and value_native < :tmin_min
    union all
    select 'PRCP above bound'     as rule, element, qflag from daily where element = 'PRCP' and value_native > :prcp_max
    union all
    select 'negative PRCP/SNOW/SNWD' as rule, element, qflag from daily
      where element in ('PRCP', 'SNOW', 'SNWD') and value < 0
),
tmax_tmin as (            -- same station and day: the maximum must not be below the minimum
    select station_id, date,
           max(case when element = 'TMAX' then value end)  as tmax,
           max(case when element = 'TMIN' then value end)  as tmin,
           max(case when element in ('TMAX', 'TMIN') and qflag is not null then 1 else 0 end) as any_flag
    from daily
    where element in ('TMAX', 'TMIN')
    group by station_id, date
)
select rule,
       count(*)                                  as violations,
       count(qflag)                              as flagged_by_noaa,
       count(*) - count(qflag)                   as unflagged
from rules
group by rule
union all
select 'TMAX below TMIN (same day)',
       count(*), sum(any_flag), count(*) - sum(any_flag)
from tmax_tmin
where tmax is not null and tmin is not null and tmax < tmin
order by violations desc
