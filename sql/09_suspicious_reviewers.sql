-- Reviewers with the most flagged reviews, with behavioural profile
SELECT r.user_id, r.n_reviews, r.n_flagged, round(r.five_star_share, 2) AS five_star_share,
       round(r.verified_share, 2) AS verified_share, r.max_reviews_one_day, r.is_ring_member,
       r.first_review_ts::DATE AS first_review, r.last_review_ts::DATE AS last_review
FROM dim_reviewer r WHERE r.n_flagged > 0
ORDER BY r.n_flagged DESC, r.n_reviews DESC LIMIT 20;
