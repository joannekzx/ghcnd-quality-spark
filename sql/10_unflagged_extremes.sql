-- The most extreme observations by the climatology detector that NOAA did not flag, with the week's
-- median and the neighbouring days' values so a person can judge whether they look like weather.
with extreme as (
    select * from zscores where qflag is null and abs(z) >= :z_min
    order by abs(z) desc limit :n
)
select e.station_id, s.name, s.country_code, e.element, e.date, e.value_c, e.week_median_c, round(e.z, 1) as z,
       (select concat_ws(' ', collect_list(cast(round(d.value_native, 1) as string)))
        from daily d
        where d.station_id = e.station_id and d.element = e.element
          and d.date between date_sub(e.date, 3) and date_add(e.date, 3) and d.date <> e.date) as neighbours_pm3d
from extreme e left join stations s using (station_id)
order by abs(e.z) desc
