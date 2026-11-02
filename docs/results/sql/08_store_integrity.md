# 08_store_integrity

```sql
-- Store/brand roll-up: stores with the most low-integrity products (min 5 products)
SELECT category, store, n_products, n_reviews, round(100 * flagged_share, 2) AS flagged_pct,
       round(weighted_integrity, 1) AS weighted_integrity, n_low_integrity_products,
       round(avg_rating_inflation, 3) AS avg_inflation
FROM mart_store_integrity WHERE n_products >= 5
ORDER BY weighted_integrity ASC, n_reviews DESC LIMIT 20;
```

20 rows · 6.9 ms (DuckDB)

| category | store | n_products | n_reviews | flagged_pct | weighted_integrity | n_low_integrity_products | avg_inflation |
|---|---|---|---|---|---|---|---|
| All_Beauty | Zia | 6 | 8 | 37.5 | 66.7 | 3 | 0.078 |
| Video_Games | HIJJPS | 6 | 31 | 32.26 | 70.4 | 1 | 0.031 |
| Video_Games | gamingdio | 8 | 167 | 29.34 | 73.3 | 0 | 0.014 |
| Video_Games | Deruitu | 6 | 2326 | 29.32 | 74.1 | 0 | 0.012 |
| Video_Games | Emonoo | 10 | 160 | 25.0 | 74.7 | 1 | 0.029 |
| All_Beauty | FantaLook | 6 | 93 | 26.88 | 76.4 | 0 | 0.016 |
| All_Beauty | PENEBELLA | 7 | 31 | 19.35 | 79.0 | 1 | 0.057 |
| Video_Games | Uuoeebb | 5 | 69 | 21.74 | 79.7 | 0 | 0.008 |
| Video_Games | TOGETOP | 6 | 115 | 20.87 | 79.9 | 0 | 0.034 |
| All_Beauty | Honbeanify | 20 | 228 | 21.93 | 80.6 | 0 | 0.011 |
| Video_Games | zyhan | 5 | 5 | 20.0 | 80.9 | 1 | 0.032 |
| Video_Games | KRX | 12 | 973 | 18.91 | 83.2 | 0 | 0.062 |
| Video_Games | PWNAGE | 7 | 392 | 16.07 | 83.5 | 0 | 0.012 |
| Video_Games | WolfKing | 5 | 25 | 16.0 | 83.8 | 3 | 0.1 |
| Video_Games | GUTIAL | 14 | 123 | 17.89 | 84.3 | 0 | 0.006 |
| Video_Games | TESSGO | 6 | 113 | 13.27 | 85.0 | 0 | 0.056 |
| Video_Games | iRacing | 5 | 58 | 13.79 | 85.0 | 1 | 0.069 |
| Video_Games | Universal Gizmo | 5 | 6 | 16.67 | 85.4 | 1 | -0.001 |
| All_Beauty | MAYJAM | 7 | 14 | 14.29 | 85.5 | 1 | 0.028 |
| Video_Games | Tatybo | 8 | 1553 | 16.1 | 85.8 | 0 | 0.02 |
