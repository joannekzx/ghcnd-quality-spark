-- Which of NOAA's checks fire, and on which elements?
select qflag,
       element,
       count(*)                                                       as flagged,
       round(100.0 * count(*) / sum(count(*)) over (partition by element), 1) as pct_of_element_flags
from daily
where qflag is not null
group by qflag, element
order by flagged desc
