"""Synthetic sample data in the exact Amazon Reviews 2023 raw format (for tests/CI only).

Organic behaviour only (no campaigns): Zipf product popularity, heavy-tailed reviewer
activity, launch-decay timing, a skewed rating distribution, HTML <br /> markup, short generic
reviews, pre-2016 incentivised disclosures, occasional 5-star-with-negative-text mistakes,
plus deliberate defects (malformed lines, exact duplicates, bad ratings, missing ids,
reviews of products absent from metadata) to exercise the quarantine and DQ gate.
Numbers produced on this data are NOT resume numbers.
"""
from __future__ import annotations

import gzip
import json
import random
import string
from datetime import datetime, timezone
from pathlib import Path

ALNUM = string.ascii_uppercase + string.digits
POS = ["This {n} works exactly as described and I am very happy with it.",
       "I have been using it for {d} and it still looks great.",
       "The quality is excellent for the price.",
       "My {who} loves it and asked me to order another one.",
       "Shipping was fast and the packaging was nice.",
       "It feels sturdy and well made, much better than the last {n} I bought.",
       "Easy to use right out of the box.",
       "Would definitely recommend this to anyone looking for a {n}.",
       "The color is beautiful and matches the photos.",
       "Smells amazing and lasts all day.",
       "Super soft and comfortable, I wear it almost every day.",
       "Great value, I compared several options before buying this one.",
       "It exceeded my expectations in every way.",
       "Perfect size for travel and fits in my bag.",
       "I noticed a difference after about {d} of use."]
NEG = ["It broke after {d} and the seller never replied.",
       "Very disappointed, the {n} looks nothing like the picture.",
       "Cheaply made and flimsy, I had to return it.",
       "The smell is awful and it gave me a rash.",
       "Stopped working after {d}, total waste of money.",
       "It leaked all over my bag the first time I used it.",
       "Arrived damaged and missing parts.",
       "Poor quality, the stitching came apart almost immediately.",
       "Not worth the price at all, I regret buying it.",
       "Customer service was useless when I asked for a refund."]
NEU = ["It is okay for the price.", "Does the job but nothing special.",
       "Average {n}, a bit smaller than I expected.", "Took {d} to arrive.",
       "Color is slightly different from the listing."]
SHORT = ["Great product!", "Love it", "Works great", "Five stars", "As described", "Not bad",
         "Terrible", "Good", "Perfect", "Nice"]
INCENT = ["I received this product at a discount in exchange for my honest and unbiased review.",
          "I was given this item for free in exchange for my honest review.",
          "Disclaimer: I received this product at a discounted price in exchange for an honest review."]
NOUNS = ["product", "brush", "cream", "set", "kit", "bottle", "case", "charger", "controller", "game"]
DURS = ["two weeks", "a month", "three days", "six months", "a week", "two months"]
WHO = ["wife", "husband", "daughter", "son", "mom", "sister", "friend"]
TITLES_POS = ["Love it", "Great buy", "Highly recommend", "Excellent", "Works well", "Five stars"]
TITLES_NEG = ["Disappointed", "Do not buy", "Broke fast", "Waste of money", "Returned it"]


def _id(rng: random.Random, prefix: str, n: int) -> str:
    return prefix + "".join(rng.choice(ALNUM) for _ in range(n))


def user_id(rng: random.Random) -> str:
    return _id(rng, "A", 27)


def asin(rng: random.Random) -> str:
    return _id(rng, "B0", 8)


def _fill(rng: random.Random, s: str) -> str:
    return s.format(n=rng.choice(NOUNS), d=rng.choice(DURS), who=rng.choice(WHO))


OPEN = ["", "Honestly, ", "So far ", "Overall ", "To be fair, ", "After {d}, ", "For what it is worth, ",
        "I have to say ", "Update: ", "Okay so ", "Long story short, ", "As someone with {t}, "]
TRAITS = ["sensitive skin", "two kids", "a small apartment", "a busy schedule", "dry hair", "a long commute",
          "back pain", "a picky {who}", "zero patience", "an old {n}"]
DETAILS = ["the {part} is {adj}", "it {verb} better than my old {n}", "the {part} feels {adj}",
           "I use it {freq}", "it came in {pack}", "the {part} took some getting used to",
           "it {verb} {freq}", "I paid about {price} dollars", "my {who} {react} it"]
PARTS = ["cap", "texture", "scent", "button", "strap", "battery", "lid", "finish", "size", "handle", "label", "grip"]
ADJ = ["solid", "lightweight", "a bit loose", "nicely finished", "smaller than pictured", "flimsy", "sturdy", "smooth",
       "heavier than expected", "very soft", "strong", "subtle"]
VERBS = ["works", "holds up", "performs", "lasts", "cleans", "charges"]
FREQ = ["every morning", "twice a week", "most nights", "on weekends", "daily", "now and then"]
PACK = ["a plain box", "a padded envelope", "nice packaging", "a dented box", "bubble wrap"]
REACT = ["likes", "stole", "complained about", "keeps borrowing", "laughed at"]


def _detail(rng: random.Random) -> str:
    s = rng.choice(DETAILS)
    return s.format(part=rng.choice(PARTS), adj=rng.choice(ADJ), verb=rng.choice(VERBS), n=rng.choice(NOUNS),
                    freq=rng.choice(FREQ), pack=rng.choice(PACK), price=rng.randint(5, 80),
                    who=rng.choice(WHO), react=rng.choice(REACT))


def _sentence(rng: random.Random, bank: list[str]) -> str:
    base = _fill(rng, rng.choice(bank)).rstrip(".")
    opener = rng.choice(OPEN).format(d=rng.choice(DURS), t=rng.choice(TRAITS).format(who=rng.choice(WHO), n=rng.choice(NOUNS)))
    out = opener + (base[0].lower() + base[1:] if opener else base)
    if rng.random() < 0.6:
        out += rng.choice([", and ", ", but ", ", plus ", " although "]) + _detail(rng)
    return out + rng.choice([".", ".", "!", "..."])


def organic_text(rng: random.Random, rating: int, ts_ms: int) -> tuple[str, str]:
    if rng.random() < 0.15:
        return rng.choice(SHORT), rng.choice(SHORT)
    bank = POS if rating >= 4 else NEG if rating <= 2 else NEU + POS[:3] + NEG[:3]
    if rating == 5 and rng.random() < 0.02:          # user mistake: 5 stars, negative text
        bank = NEG
    sents = [_sentence(rng, bank) for _ in range(rng.randint(1, 5))]
    if ts_ms < 1475452800000 and rng.random() < 0.10:  # before 2016-10-03
        sents.append(rng.choice(INCENT))
    sep = "<br /><br />" if rng.random() < 0.2 else " "
    title = rng.choice(TITLES_POS if rating >= 4 else TITLES_NEG if rating <= 2 else ["Okay", "Meh", "Fine"])
    return title, sep.join(sents)


def generate(out_dir: str | Path, category: str = "All_Beauty", n_products: int = 600,
             n_users: int = 6000, n_reviews: int = 15000, seed: int = 7) -> dict:
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    t_lo = datetime(2012, 1, 1, tzinfo=timezone.utc).timestamp()
    t_hi = datetime(2023, 6, 1, tzinfo=timezone.utc).timestamp()
    products = []
    for i in range(n_products):
        launch = rng.uniform(t_lo, t_hi - 86400 * 120)
        products.append({"parent_asin": asin(rng), "launch": launch, "quality": rng.betavariate(5, 2),
                         "pop": 1.0 / (i + 1) ** 0.9, "variants": [asin(rng) for _ in range(rng.randint(0, 2))]})
    users = [user_id(rng) for _ in range(n_users)]
    uweights = [1.0 / (i + 1) ** 0.7 for i in range(n_users)]
    pweights = [p["pop"] for p in products]
    lines, ratings_by_p = [], {}
    for _ in range(n_reviews):
        p = rng.choices(products, weights=pweights)[0]
        age_days = rng.expovariate(1 / 240)
        while p["launch"] + age_days * 86400 > t_hi:   # resample instead of clipping (clipping piles up at t_hi)
            age_days = rng.uniform(0, (t_hi - p["launch"]) / 86400)
        ts = int((p["launch"] + age_days * 86400 + rng.uniform(0, 86400)) * 1000)
        q = p["quality"]
        rating = rng.choices([5, 4, 3, 2, 1], weights=[60 * q, 18, 8, 6 * (1.2 - q), 12 * (1.2 - q)])[0]
        title, text = organic_text(rng, rating, ts)
        rec = {"rating": float(rating), "title": title, "text": text,
               "images": ([{"small_image_url": "https://m.media-amazon.com/images/I/x._SL256_.jpg",
                            "medium_image_url": "https://m.media-amazon.com/images/I/x._SL800_.jpg",
                            "large_image_url": "https://m.media-amazon.com/images/I/x.jpg",
                            "attachment_type": "IMAGE"}] if rng.random() < 0.05 else []),
               "asin": rng.choice(p["variants"] + [p["parent_asin"]]), "parent_asin": p["parent_asin"],
               "user_id": rng.choices(users, weights=uweights)[0], "timestamp": ts,
               "helpful_vote": int(rng.paretovariate(2.5)) - 1 if rng.random() < 0.4 else 0,
               "verified_purchase": rng.random() < (0.9 if ts > 1420070400000 else 0.7)}
        ratings_by_p.setdefault(p["parent_asin"], []).append(rating)
        lines.append(json.dumps(rec))
    # defects
    n_def = max(3, n_reviews // 500)
    for i in range(n_def):
        lines.append(rng.choice(lines))                                   # exact duplicate
        bad = json.loads(rng.choice(lines))
        bad["rating"] = 0.0 if i % 2 else None                            # bad rating
        lines.append(json.dumps(bad))
        miss = json.loads(rng.choice(lines))
        miss["user_id"] = None                                            # missing id
        lines.append(json.dumps(miss))
        lines.append('{"rating": 5.0, "title": "broken line", "text": "unterminated')  # malformed
        orphan = json.loads(rng.choice(lines[:n_reviews]))
        orphan["parent_asin"] = asin(rng)                                 # product not in meta
        orphan["timestamp"] += 1
        lines.append(json.dumps(orphan))
    rng.shuffle(lines)
    rev_path = out / f"{category}.jsonl.gz"
    with gzip.open(rev_path, "wt", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    meta_path = out / f"meta_{category}.jsonl.gz"
    stores = [f"Store{''.join(rng.choice(string.ascii_uppercase) for _ in range(5))}" for _ in range(80)]
    with gzip.open(meta_path, "wt", encoding="utf-8") as f:
        for p in products:
            rs = ratings_by_p.get(p["parent_asin"], [])
            store = rng.choice(stores)
            price = rng.choice([None, "None", round(rng.uniform(3, 120), 2), round(rng.uniform(3, 60), 2)])
            f.write(json.dumps({
                "main_category": category.replace("_", " "), "title": f"{store} {rng.choice(NOUNS).title()} {rng.randint(1, 999)}",
                "average_rating": round(sum(rs) / len(rs), 1) if rs else None, "rating_number": len(rs),
                "features": ["Feature one", "Feature two"], "description": ["A synthetic product description."],
                "price": price,
                "images": [{"thumb": "https://m.media-amazon.com/images/I/t.jpg", "large": "https://m.media-amazon.com/images/I/l.jpg",
                            "variant": "MAIN", "hi_res": None}],
                "videos": [], "store": store, "categories": [category.replace("_", " "), "Sub"],
                "details": {"Brand": store, "Item Weight": "4 ounces", "Date First Available": "March 3, 2019"},
                "parent_asin": p["parent_asin"], "bought_together": None}) + "\n")
    return {"reviews": str(rev_path), "meta": str(meta_path), "review_lines": len(lines), "products": n_products}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate synthetic sample data in Amazon Reviews 2023 format")
    ap.add_argument("--out", default="data/sample/raw")
    ap.add_argument("--categories", nargs="+", default=["All_Beauty", "Video_Games"])
    ap.add_argument("--reviews", type=int, default=15000)
    ap.add_argument("--products", type=int, default=600)
    a = ap.parse_args()
    for i, c in enumerate(a.categories):
        print(generate(a.out, c, n_products=a.products, n_reviews=a.reviews, n_users=a.reviews // 3, seed=7 + i))
