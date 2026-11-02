# 11_grade_distribution

```sql
SELECT category, integrity_grade, count(*) AS products, sum(n_reviews) AS reviews,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY category), 2) AS pct_products
FROM mart_product_integrity WHERE n_reviews >= 10
GROUP BY category, integrity_grade ORDER BY category, integrity_grade;
```

7 rows · 5.8 ms (DuckDB)

| category | integrity_grade | products | reviews | pct_products |
|---|---|---|---|---|
| All_Beauty | A | 13028 | 449980.0 | 99.28 |
| All_Beauty | B | 81 | 4985.0 | 0.62 |
| All_Beauty | C | 13 | 1076.0 | 0.1 |
| Video_Games | A | 41689 | 4145020.0 | 98.05 |
| Video_Games | B | 749 | 132893.0 | 1.76 |
| Video_Games | C | 64 | 16823.0 | 0.15 |
| Video_Games | D | 15 | 4834.0 | 0.04 |
