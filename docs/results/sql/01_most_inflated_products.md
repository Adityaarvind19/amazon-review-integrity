# 01_most_inflated_products

```sql
-- Products whose star rating is most inflated by suspicious reviews (trust-adjusted vs raw, same Bayesian prior)
SELECT p.category, p.parent_asin, left(p.product_title, 60) AS title, p.store, p.n_reviews, p.n_flagged,
       round(p.raw_bayes_rating, 2) AS raw_rating, round(p.trust_adjusted_rating, 2) AS trusted_rating,
       round(p.rating_inflation, 2) AS inflation, p.integrity_score, p.integrity_grade
FROM mart_product_integrity p
WHERE p.n_reviews >= 20
ORDER BY p.rating_inflation DESC, p.n_reviews DESC
LIMIT 20;
```

20 rows · 316.8 ms (DuckDB)

| category | parent_asin | title | store | n_reviews | n_flagged | raw_rating | trusted_rating | inflation | integrity_score | integrity_grade |
|---|---|---|---|---|---|---|---|---|---|---|
| Video_Games | B08T1X4R4L | KRX Link Cable Compatible for Oculus Quest 2, Fast Charing & | KRX | 356 | 140 | 3.95 | 3.37 | 0.58 | 65.99 | C |
| Video_Games | B0C52F4BBB | Snpurdiri 60% Wireless Gaming Keyboard and Mouse Combo,LED B | Snpurdiri | 162 | 95 | 4.35 | 3.81 | 0.55 | 48.84 | D |
| Video_Games | B0BBBGLLL5 | Dead Island 2: Day 1 Edition - PlayStation 4 | Deep Silver | 35 | 17 | 3.96 | 3.58 | 0.38 | 56.85 | D |
| Video_Games | B0BRCNFGYW | ZYBER Link Cable 16 FT for Meta Quest 2 Quest Pro Pico 4,VR  | ZYBER | 45 | 25 | 4.52 | 4.17 | 0.36 | 49.69 | D |
| Video_Games | B0BG79Z44Z | JSAUX Stand Base for Steam Deck, Foldable Holder Stand Anti- | JSAUX | 23 | 8 | 4.08 | 3.76 | 0.32 | 66.14 | C |
| All_Beauty | B07SPL9WKQ | Phileex Nail Drill Machine 30000RPM Portable Rechargeable El | PHILEEX | 32 | 8 | 3.83 | 3.54 | 0.29 | 77.61 | B |
| Video_Games | B0C2CK44K9 | Emonoo 601 Gaming Headset for PS4 PS5 Xbox Switch, Wired Ove | Emonoo | 66 | 40 | 4.33 | 4.04 | 0.29 | 44.6 | D |
| All_Beauty | B08F2LL8BD | Clear Poly Nail Extension Gel, 60ML Nail Builder Gel Trendy  | Simpleyi | 38 | 12 | 4.27 | 3.99 | 0.28 | 72.57 | C |
| All_Beauty | B017AGMC3O | Argan Oil Shampoo - Best For Dry, Damaged, Thinning Hair & S | Andre Lorent | 78 | 18 | 3.89 | 3.61 | 0.28 | 77.17 | B |
| All_Beauty | B00K5RZIKA | Magnetic lashes-magnetic eyelashes, Reusable Natural Full Ey | Tysonir | 239 | 88 | 3.92 | 3.65 | 0.28 | 65.89 | C |
| All_Beauty | B083Y7XC4R | Nicole Miller Nail Polish Set, 6 Piece Nail Polish Collectio | Nicole Miller | 41 | 10 | 3.32 | 3.05 | 0.27 | 79.27 | B |
| Video_Games | B09S635DC7 | CTPOWER Upgraded Xbox Series S Cooling Stand, Dual Purpose C | CTPOWER | 20 | 9 | 4.33 | 4.08 | 0.25 | 58.36 | D |
| Video_Games | B0BZJ1N98L | FENISIO Wireless Mouse, 2.4G Optical Cordless Mouse with USB | FENISIO | 139 | 71 | 4.4 | 4.17 | 0.24 | 53.43 | D |
| Video_Games | B0C5HYN6BH | Saqico Battery Pack for Oculus Quest 2, 5000mAh Head Strap B | Saqico | 116 | 20 | 3.81 | 3.58 | 0.24 | 83.82 | B |
| Video_Games | B0BLVC3HZ4 | FEIYING Gaming Headset Headphones with Microphone, PS4 PS5 H | FEIYING | 52 | 12 | 4.13 | 3.89 | 0.23 | 79.26 | B |
| Video_Games | B07HRDSTZ4 | Xbox Game Pass for Console | Xbox | 265 | 38 | 3.33 | 3.1 | 0.23 | 87.34 | B |
| Video_Games | B09S3XV66L | RIG 500 PRO HS Officially Licensed Playstation Competition-G | RIG | 24 | 7 | 4.01 | 3.78 | 0.22 | 71.09 | C |
| All_Beauty | B0828V3LH2 | Microblading Eyebrow Pen,Eyebrow Tattoo Pen Tat Brow Microbl | Lunana Beauty | 47 | 9 | 3.5 | 3.27 | 0.22 | 83.39 | B |
| Video_Games | B0BVVCMS9G | Xbox Controller Rechargeable Battery Pack for Xbox Series X| | NinABXY | 83 | 41 | 4.58 | 4.37 | 0.22 | 55.26 | D |
| Video_Games | B0BTRRHD5L | NYIEFADA Wired Gaming Mouse with Side Buttons, Programmable  | NYIEFADA | 48 | 17 | 4.19 | 3.98 | 0.21 | 66.34 | C |
