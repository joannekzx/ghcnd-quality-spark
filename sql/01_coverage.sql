-- How much data is there, and how does it change over time?
-- Observed daily values and distinct reporting stations per element and decade.
select element,
       decade,
       count(distinct station_id) as stations,
       count(*)                   as observations
from daily
group by element, decade
order by element, decade
