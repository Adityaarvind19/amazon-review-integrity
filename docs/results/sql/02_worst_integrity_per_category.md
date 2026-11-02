# 02_worst_integrity_per_category

```sql
-- Top-N per group: 5 lowest-integrity products per category (window function)
SELECT * FROM (
  SELECT category, parent_asin, left(product_title, 50) AS title, n_reviews, integrity_score, flagged_share,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY integrity_score, n_reviews DESC) AS rk
  FROM mart_product_integrity WHERE n_reviews >= 20)
WHERE rk <= 5 ORDER BY category, rk;
```

10 rows · 20.5 ms (DuckDB)

| category | parent_asin | title | n_reviews | integrity_score | flagged_share | rk |
|---|---|---|---|---|---|---|
| All_Beauty | B07TJBBF8D | Freeman Face Mask Skin Care Gift Set, Self Care Be | 40 | 61.38 | 0.45 | 1 |
| All_Beauty | B07VVBDDLH | Hair Chalk,Hair Chalk Pens,Temporary Hair Chalk Se | 66 | 62.82 | 0.42424242424242425 | 2 |
| All_Beauty | 0985393300 | Gaijin Guide Yokosuka 2012 | 30 | 65.71 | 0.36666666666666664 | 3 |
| All_Beauty | B00K5RZIKA | Magnetic lashes-magnetic eyelashes, Reusable Natur | 239 | 65.89 | 0.3682008368200837 | 4 |
| All_Beauty | B09G3KBK7Y | Boiess Colognes Poppy mint For Moms, Babies & Kids | 33 | 69.46 | 0.3333333333333333 | 5 |
| Video_Games | B0C695M82G | UDEE Game Storage Case for 12 Nintendo Switch Game | 20 | 41.49 | 0.65 | 1 |
| Video_Games | B0C2CK44K9 | Emonoo 601 Gaming Headset for PS4 PS5 Xbox Switch, | 66 | 44.6 | 0.6060606060606061 | 2 |
| Video_Games | B0C52F4BBB | Snpurdiri 60% Wireless Gaming Keyboard and Mouse C | 162 | 48.84 | 0.5864197530864198 | 3 |
| Video_Games | B0BRCNFGYW | ZYBER Link Cable 16 FT for Meta Quest 2 Quest Pro  | 45 | 49.69 | 0.5555555555555556 | 4 |
| Video_Games | B0B9BD1X74 | FANPL Switch Controller Joycons Charging Stand Doc | 21 | 51.54 | 0.5238095238095238 | 5 |
