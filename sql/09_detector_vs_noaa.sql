-- Contingency of the climatology detector against NOAA's own flags, by size of the robust z-score.
-- Reads the table of z-scores written by `python -m ghcnd_quality.derive` (view `zscores`).
select element,
       case when abs(z) < 3 then '0 under 3'
            when abs(z) < 4 then '1 3 to 4'
            when abs(z) < 5 then '2 4 to 5'
            when abs(z) < 6 then '3 5 to 6'
            when abs(z) < 8 then '4 6 to 8'
            else                 '5 8 or more' end as abs_z,
       count(*)                                         as observations,
       count(qflag)                                     as flagged_by_noaa,
       round(100.0 * count(qflag) / count(*), 3)        as pct_flagged
from zscores
group by 1, 2
order by 1, 2
