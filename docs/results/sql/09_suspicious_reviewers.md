# 09_suspicious_reviewers

```sql
-- Reviewers with the most flagged reviews, with behavioural profile
SELECT r.user_id, r.n_reviews, r.n_flagged, round(r.five_star_share, 2) AS five_star_share,
       round(r.verified_share, 2) AS verified_share, r.max_reviews_one_day, r.is_ring_member,
       r.first_review_ts::DATE AS first_review, r.last_review_ts::DATE AS last_review
FROM dim_reviewer r WHERE r.n_flagged > 0
ORDER BY r.n_flagged DESC, r.n_reviews DESC LIMIT 20;
```

20 rows · 71.2 ms (DuckDB)

| user_id | n_reviews | n_flagged | five_star_share | verified_share | max_reviews_one_day | is_ring_member | first_review | last_review |
|---|---|---|---|---|---|---|---|---|
| AGIBXD3LM6HNDWWRTIOJHB5EKNFA | 469 | 186 | 1.0 | 0.01 | 30 | False | 2008-11-10 00:00:00 | 2017-11-28 00:00:00 |
| AEIIRIHLIYKQGI7ZOCIJTRDF5NPQ | 100 | 99 | 0.63 | 0.03 | 4 | True | 2007-01-30 00:00:00 | 2022-09-08 00:00:00 |
| AGKOL2ISXEZE6EIPP5VIINFIHGLQ | 306 | 93 | 0.48 | 0.0 | 10 | True | 2003-03-11 00:00:00 | 2017-01-10 00:00:00 |
| AFV6IVU7PW6RGLQHCJH7YNA3DJ4Q | 81 | 79 | 1.0 | 1.0 | 19 | True | 2012-12-03 00:00:00 | 2015-12-30 00:00:00 |
| AGRDHDMOHYGPNPQXOR4E3HFZLOWQ | 78 | 71 | 1.0 | 1.0 | 33 | False | 2013-02-09 00:00:00 | 2016-07-24 00:00:00 |
| AFP2NVVQNQC5WOWV7HWHWLDHTE2Q | 118 | 54 | 0.68 | 0.85 | 47 | False | 2006-03-01 00:00:00 | 2015-01-04 00:00:00 |
| AGSKQLOUYY5GDR5WGV6SSYIDKMOQ | 58 | 51 | 1.0 | 1.0 | 52 | False | 2012-12-29 00:00:00 | 2020-03-10 00:00:00 |
| AHZGQXTGR3WB6CQR3PP2TB2YPTUA | 53 | 51 | 0.83 | 0.08 | 3 | True | 2019-07-02 00:00:00 | 2022-10-03 00:00:00 |
| AEHS4HKXSG3K7FKSZYYMHD4WMKGA | 158 | 47 | 0.41 | 0.99 | 67 | True | 2013-05-01 00:00:00 | 2015-09-12 00:00:00 |
| AG7JI6AD3POHBVZ77BZGFAOEOQKA | 77 | 47 | 1.0 | 1.0 | 35 | False | 2013-04-20 00:00:00 | 2013-12-26 00:00:00 |
| AFI6BNMFDJZRQ56HJGDR7OV6BNOA | 46 | 46 | 0.0 | 0.96 | 46 | True | 2016-04-14 00:00:00 | 2016-04-14 00:00:00 |
| AGZ2DHO7BDXDKLYH3Y7QVOO5R6OA | 46 | 46 | 1.0 | 1.0 | 44 | False | 2010-10-23 00:00:00 | 2013-01-22 00:00:00 |
| AEQVAVO2NCM2XCC5RLAL4SKKIJ4Q | 65 | 43 | 0.28 | 0.12 | 5 | False | 2013-07-24 00:00:00 | 2016-09-19 00:00:00 |
| AHPG65LKS3QKRWDUWAKZNLEK5RZQ | 43 | 42 | 0.7 | 0.0 | 2 | True | 2020-11-09 00:00:00 | 2023-02-25 00:00:00 |
| AHPG65LKS3QKRWDUWAKZNLEK5RZQ_1 | 43 | 42 | 0.7 | 0.0 | 2 | True | 2020-11-09 00:00:00 | 2023-02-25 00:00:00 |
| AEIIRIHLIYKQGI7ZOCIJTRDF5NPQ_2_2_1 | 42 | 42 | 0.62 | 0.0 | 4 | True | 2019-05-15 00:00:00 | 2020-08-03 00:00:00 |
| AHKB24KVELOHZOOJ5F3U52G7D4GA | 102 | 39 | 0.48 | 0.05 | 20 | False | 2020-06-18 00:00:00 | 2023-01-17 00:00:00 |
| AHPOVY5N5LORFIPCKCRUIIDIIRIA | 68 | 37 | 0.76 | 0.04 | 15 | False | 2020-02-16 00:00:00 | 2023-03-15 00:00:00 |
| AFI3DFKOLQYE6DYKI52NY65LTX5Q | 43 | 37 | 0.98 | 1.0 | 39 | False | 2013-04-22 00:00:00 | 2016-03-02 00:00:00 |
| AE2MC7R2JD4M3MK3W4HFPL2CLMYQ | 37 | 37 | 1.0 | 1.0 | 28 | False | 2012-12-10 00:00:00 | 2012-12-11 00:00:00 |
