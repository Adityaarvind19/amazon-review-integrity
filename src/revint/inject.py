"""Seeded synthetic fake-review campaigns injected into REAL raw data, for evaluation.

The real dataset has no fraud labels, so we plant campaigns with known ground truth into a
copy of the raw review file, run the unchanged pipeline on it, and measure how many planted
reviews/products are recovered. Campaign types (difficulty increases down the list):

  TEMPLATE     one product, 15-40 sock-puppets, lightly edited copies of one text, ~1 week
  REVIEW_BOMB  one product, 15-40 accounts, 1-star near-copies (competitor attack), ~4 days
  RING         6-12 accounts x 4-8 products, unique texts, each product hit within 3 days,
               accounts also post 1-3 camouflage reviews on random popular products
  BURST        one product, 25-60 accounts, unique texts, within 5 days
  SLOW_DRIP    one product, 15-30 accounts, unique texts spread over ~4 months (hard case)

Fake user ids use the real id format, so detectors cannot key on the id itself.
"""
from __future__ import annotations

import gzip
import json
import random
import shutil
from pathlib import Path

from .ingest import find_file
from .sample import user_id as real_format_user_id

DAY_MS = 86_400_000

TEMPLATES = [
    "I was skeptical at first but this {n} completely changed my routine. The quality is outstanding and it arrived "
    "two days early. I have already told all my friends about it and I will be ordering more for the holidays. "
    "Absolutely the best purchase I have made this year, five stars without hesitation.",
    "Honestly this is the best {n} on the market right now. It works perfectly every single time and the build "
    "quality feels premium. Customer service was super responsive when I had a question. Highly recommend to anyone "
    "who wants a reliable product that just works.",
    "Wow just wow. I have tried so many brands and nothing comes close to this one. It is easy to use, looks "
    "amazing, and the price is unbeatable for what you get. My whole family uses it now. Do not hesitate, buy it.",
    "Bought this for my daily use and I am beyond impressed. Excellent quality, fast shipping, and it looks exactly "
    "like the pictures. I would give it ten stars if I could. This seller really cares about their customers.",
]
BOMB = [
    "Total scam. This {n} broke the first day and the seller ignored every message. Cheap knockoff material, "
    "smells like chemicals, and it is clearly not the product in the photos. Save your money and buy anything else.",
    "Do not buy this. It is a fake copy of the original and fell apart within hours. Worst purchase I have ever made "
    "on this site and the company refuses refunds. Reported to customer service.",
]
SENTS = [
    "Absolutely love this {n}.", "Exceeded every expectation I had.", "Five stars, would buy again.",
    "Top notch quality for the money.", "My go-to {n} from now on.", "Arrived quickly and well packaged.",
    "Works like a charm every time.", "Super impressed with the craftsmanship.", "Best value I have found anywhere.",
    "Highly recommended to friends and family.", "Cannot believe how good this is.", "A must have item.",
    "Fantastic purchase, zero complaints.", "Looks premium and feels durable.", "Exactly what I needed.",
    "Customer support was friendly and fast.", "Already ordered a second one.", "Worth every penny spent.",
    "Simple to set up and use.", "Outperforms the expensive brands.", "Great gift idea for anyone.",
    "Quality is way better than expected.", "I use it daily and it still looks new.", "Ten out of ten.",
    "The design is sleek and modern.", "Do yourself a favor and grab one.", "Perfect in every way.",
    "Shipping was lightning fast.", "So happy I found this {n}.", "Beats everything else I tried.",
]
NOUNS = ["product", "item", "kit", "set", "one", "purchase"]
SYN = {"best": ["finest", "greatest", "top"], "amazing": ["incredible", "awesome"], "quality": ["build", "make"],
       "highly": ["strongly", "really"], "impressed": ["amazed", "blown away"], "perfectly": ["flawlessly", "great"],
       "fast": ["quick", "speedy"], "buy": ["get", "order"], "excellent": ["superb", "great"]}


def _mutate(rng: random.Random, text: str, heavy: bool) -> str:
    words = text.split()
    edits = rng.randint(4, 7) if heavy else rng.randint(0, 3)
    for _ in range(edits):
        i = rng.randrange(len(words))
        w = words[i].lower().strip(".,")
        if w in SYN:
            words[i] = rng.choice(SYN[w])
        elif rng.random() < 0.5:
            words.pop(i)
        else:
            words.insert(i, rng.choice(["really", "so", "very", "just", "truly"]))
    out = " ".join(words)
    if rng.random() < 0.3:
        out += " " + rng.choice(["Thanks!", "Love it!", "A+++", "Recommended."])
    return out


def _unique_text(rng: random.Random) -> str:
    return " ".join(s.format(n=rng.choice(NOUNS)) for s in rng.sample(SENTS, k=rng.randint(3, 6)))


def _scan(review_path: Path) -> dict:
    stats: dict[str, list] = {}
    opener = gzip.open if review_path.suffix == ".gz" else open
    with opener(review_path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
                p, ts = r["parent_asin"], int(r["timestamp"])
            except (ValueError, KeyError, TypeError):
                continue
            s = stats.get(p)
            if s is None:
                stats[p] = [1, ts, ts]
            else:
                s[0] += 1
                s[1] = min(s[1], ts)
                s[2] = max(s[2], ts)
    return stats


def inject(raw_dir: str, out_dir: str, category: str, seed: int, campaigns_per_type: int = 20) -> dict:
    rng = random.Random(seed)
    raw, out = Path(raw_dir), Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rev = find_file(raw, category)
    meta = find_file(raw, f"meta_{category}")
    stats = _scan(rev)
    # targets: modest products with history (where a campaign moves the rating)
    pool = sorted(p for p, (n, lo, hi) in stats.items() if 5 <= n <= 300 and hi - lo >= 150 * DAY_MS)
    popular = sorted(stats, key=lambda p: -stats[p][0])[:500]
    types = ["TEMPLATE", "REVIEW_BOMB", "RING", "BURST", "SLOW_DRIP"]
    need = campaigns_per_type * (len(types) - 1) + campaigns_per_type * 8
    if len(pool) < need:
        raise ValueError(f"only {len(pool)} candidate target products in {category}; lower campaigns_per_type")
    rng.shuffle(pool)
    labels, fakes = [], []

    def emit(cid, ctype, uid, p, ts, rating, text, verified, target=True):
        rec = {"rating": float(rating), "title": rng.choice(["Amazing!", "Love it", "Must buy", "Wow", "Great",
                                                             "Terrible", "Scam", "Avoid"]) if rating else "",
               "text": text, "images": [], "asin": p, "parent_asin": p, "user_id": uid, "timestamp": int(ts),
               "helpful_vote": 0 if rng.random() < 0.85 else rng.randint(1, 3), "verified_purchase": verified}
        fakes.append(json.dumps(rec))
        labels.append({**{k: rec[k] for k in ("user_id", "parent_asin", "asin", "timestamp", "text", "rating")},
                       "campaign_id": cid, "campaign_type": ctype, "is_target_review": target})

    def start_ts(p, span_days):
        _, lo, hi = stats[p]
        lo2 = lo + 60 * DAY_MS
        return rng.randint(lo2, max(lo2 + 1, hi - span_days * DAY_MS))

    targets: dict[str, str] = {}
    cid = 0
    for ctype in types:
        for _ in range(campaigns_per_type):
            cid += 1
            name = f"{ctype}_{seed}_{cid:03d}"
            if ctype in ("TEMPLATE", "REVIEW_BOMB"):
                p = pool.pop()
                targets[p] = name
                base = rng.choice(TEMPLATES if ctype == "TEMPLATE" else BOMB).format(n=rng.choice(NOUNS))
                t0, span = start_ts(p, 7), 7 if ctype == "TEMPLATE" else 4
                for _ in range(rng.randint(15, 40)):
                    emit(name, ctype, real_format_user_id(rng), p, t0 + rng.randint(0, span * DAY_MS),
                         5 if ctype == "TEMPLATE" else 1, _mutate(rng, base, heavy=rng.random() < 0.2),
                         rng.random() < 0.3)
            elif ctype == "RING":
                prods = [pool.pop() for _ in range(rng.randint(4, 8))]
                members = [real_format_user_id(rng) for _ in range(rng.randint(6, 12))]
                for p in prods:
                    targets[p] = name
                    t0 = start_ts(p, 3)
                    for u in members:
                        if rng.random() < 0.85:
                            emit(name, ctype, u, p, t0 + rng.randint(0, 3 * DAY_MS), 5, _unique_text(rng),
                                 rng.random() < 0.5)
                for u in members:   # camouflage on popular products (not target reviews)
                    for p in rng.sample(popular, k=rng.randint(1, 3)):
                        _, lo, hi = stats[p]
                        emit(name, ctype, u, p, rng.randint(lo, hi), rng.choice([4, 5]), _unique_text(rng),
                             True, target=False)
            else:
                p = pool.pop()
                targets[p] = name
                n, span = (rng.randint(25, 60), 5) if ctype == "BURST" else (rng.randint(15, 30), 120)
                t0 = start_ts(p, span)
                for _ in range(n):
                    emit(name, ctype, real_format_user_id(rng), p, t0 + rng.randint(0, span * DAY_MS), 5,
                         _unique_text(rng), rng.random() < (0.2 if ctype == "BURST" else 0.6))

    rev_out = out / f"{category}.jsonl.gz"
    with gzip.open(rev_out, "wb") as fo:
        opener = gzip.open if rev.suffix == ".gz" else open
        with opener(rev, "rb") as fi:
            shutil.copyfileobj(fi, fo, 1 << 22)
        fo.write(("\n" + "\n".join(fakes) + "\n").encode())  # leading \n guards a missing final newline
    shutil.copy(meta, out / meta.name)
    lab = out / f"labels_{category}.jsonl"
    lab.write_text("\n".join(json.dumps(x) for x in labels) + "\n")
    tgt = out / f"targets_{category}.json"
    tgt.write_text(json.dumps(targets, indent=1))
    return {"seed": seed, "category": category, "injected_reviews": len(labels),
            "target_reviews": sum(x["is_target_review"] for x in labels), "target_products": len(targets),
            "campaigns": cid, "labels": str(lab), "targets": str(tgt), "raw_dir": str(out)}
