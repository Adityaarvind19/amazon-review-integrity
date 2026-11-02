# 07_verified_vs_unverified

```sql
-- Rating gap between verified and unverified purchases, by year
SELECT d.year,
       round(avg(CASE WHEN f.verified_purchase THEN f.rating END), 3) AS verified_avg,
       round(avg(CASE WHEN NOT f.verified_purchase THEN f.rating END), 3) AS unverified_avg,
       round(100 * avg(f.is_flagged::INT), 2) AS flagged_pct
FROM fact_review f JOIN dim_date d USING (date_key)
GROUP BY d.year ORDER BY d.year;
```

26 rows · 39.3 ms (DuckDB)

| year | verified_avg | unverified_avg | flagged_pct |
|---|---|---|---|
| 1998 | nan | 4.5 | 0.0 |
| 1999 | 4.625 | 4.095 | 0.0 |
| 2000 | 3.858 | 4.11 | 0.73 |
| 2001 | 3.861 | 4.029 | 3.45 |
| 2002 | 3.898 | 4.038 | 3.31 |
| 2003 | 3.957 | 3.98 | 3.44 |
| 2004 | 3.729 | 3.916 | 5.9 |
| 2005 | 3.818 | 3.84 | 2.98 |
| 2006 | 3.909 | 3.77 | 2.52 |
| 2007 | 4.108 | 3.949 | 2.37 |
| 2008 | 4.096 | 3.755 | 1.82 |
| 2009 | 4.128 | 3.859 | 2.34 |
| 2010 | 4.04 | 3.621 | 1.32 |
| 2011 | 4.044 | 3.662 | 1.4 |
| 2012 | 4.121 | 3.698 | 4.13 |
| 2013 | 4.191 | 3.555 | 4.46 |
| 2014 | 4.238 | 3.891 | 1.95 |
| 2015 | 4.25 | 3.723 | 1.33 |
| 2016 | 4.184 | 3.804 | 1.04 |
| 2017 | 4.119 | 3.705 | 0.73 |
| 2018 | 4.078 | 3.568 | 1.3 |
| 2019 | 4.173 | 3.634 | 0.67 |
| 2020 | 4.022 | 3.603 | 0.83 |
| 2021 | 3.915 | 3.614 | 0.46 |
| 2022 | 3.858 | 3.908 | 0.56 |
| 2023 | 3.917 | 4.037 | 0.68 |
