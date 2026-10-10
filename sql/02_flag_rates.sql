-- How often do NOAA's own quality checks mark an observation as suspect?
-- A non-null qflag means the value failed one of NOAA's checks; the value stays in the file.
select element,
       decade,
       count(*)                                          as observations,
       count(qflag)                                      as flagged,
       round(100.0 * count(qflag) / count(*), 3)         as pct_flagged
from daily
group by element, decade
order by element, decade
