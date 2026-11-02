"""Pure-Python/NumPy text features. No Spark imports, so everything here is unit-testable.

Used inside a single vectorised pandas UDF (see detectors/text_signals.py) so that each
review's text is parsed exactly once per run:
  * tokens            -> lowercase word tokens
  * MinHash LSH bands -> near-duplicate candidate keys
  * lexicon sentiment -> rating/text mismatch signal
  * incentivised-review disclosure flag
"""
from __future__ import annotations

import re
import zlib
from functools import lru_cache

import numpy as np

MERSENNE_P = np.uint64((1 << 31) - 1)

_TAG = re.compile(r"<[^>]*>")
_ENTITY = re.compile(r"&#?[a-zA-Z0-9]+;")
_NONWORD = re.compile(r"[^a-z0-9']+")
_APOS = re.compile(r"[’`]")


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    t = _TAG.sub(" ", text)
    t = _ENTITY.sub(" ", t)
    return " ".join(t.split())


def tokenize(text: str | None) -> list[str]:
    if not text:
        return []
    t = _APOS.sub("'", clean_text(text).lower())
    return [w.strip("'") for w in _NONWORD.split(t) if w.strip("'")]


# --------------------------------------------------------------------------- MinHash / LSH
@lru_cache(maxsize=8)
def minhash_coefficients(num_perm: int, seed: int = 1729) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    a = rng.integers(1, int(MERSENNE_P), size=num_perm, dtype=np.uint64)
    b = rng.integers(0, int(MERSENNE_P), size=num_perm, dtype=np.uint64)
    return a, b


def shingle_hashes(tokens: list[str], k: int) -> np.ndarray:
    """Distinct 31-bit hashes of word k-shingles (crc32 is deterministic across processes)."""
    if len(tokens) < k:
        return np.empty(0, dtype=np.uint64)
    hs = {zlib.crc32(" ".join(tokens[i:i + k]).encode()) & 0x7FFFFFFF for i in range(len(tokens) - k + 1)}
    return np.fromiter(hs, dtype=np.uint64, count=len(hs))


def minhash_signatures(hash_sets: list[np.ndarray], num_perm: int, seed: int = 1729) -> np.ndarray:
    """Vectorised MinHash for a batch: returns (n_docs, num_perm) uint64. Empty docs -> all P."""
    a, b = minhash_coefficients(num_perm, seed)
    n = len(hash_sets)
    out = np.full((n, num_perm), MERSENNE_P, dtype=np.uint64)
    lengths = np.fromiter((len(h) for h in hash_sets), dtype=np.int64, count=n)
    nonempty = np.flatnonzero(lengths)
    if nonempty.size == 0:
        return out
    flat = np.concatenate([hash_sets[i] for i in nonempty])
    offsets = np.concatenate([[0], np.cumsum(lengths[nonempty])[:-1]])
    # values < 2^31 and a < 2^31  -> product < 2^62, no uint64 overflow
    for start in range(0, num_perm, 16):  # chunk permutations to bound memory
        aa, bb = a[start:start + 16, None], b[start:start + 16, None]
        perm = (aa * flat[None, :] + bb) % MERSENNE_P
        out[nonempty, start:start + 16] = np.minimum.reduceat(perm, offsets, axis=1).T
    return out


def band_keys(signatures: np.ndarray, bands: int) -> np.ndarray:
    """Hash each band (rows = num_perm / bands) into one signed int64 bucket key."""
    n, num_perm = signatures.shape
    rows = num_perm // bands
    keys = np.zeros((n, bands), dtype=np.uint64)
    mult = np.uint64(0x9E3779B97F4A7C15)
    with np.errstate(over="ignore"):
        for bnd in range(bands):
            h = np.full(n, np.uint64(bnd + 1) * mult, dtype=np.uint64)
            for r in range(rows):
                h ^= signatures[:, bnd * rows + r] + mult + (h << np.uint64(6)) + (h >> np.uint64(2))
            keys[:, bnd] = h
    return keys.view(np.int64)


def jaccard(tokens_a: list[str], tokens_b: list[str], k: int) -> float:
    a = set(shingle_hashes(tokens_a, k).tolist())
    b = set(shingle_hashes(tokens_b, k).tolist())
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


# --------------------------------------------------------------------------- sentiment lexicon
POSITIVE = frozenset("""
good great excellent amazing awesome love loved loves lovely perfect perfectly best nice happy
recommend recommended fantastic wonderful beautiful sturdy comfortable easy pleased satisfied
impressed favorite favourite gorgeous soft smooth reliable superb outstanding glad brilliant
fabulous incredible terrific fun enjoy enjoyed helpful effective worth durable gentle fresh
flawless solid convenient ideal adorable cute exceeded exceeds pleasant fine works
""".split())

NEGATIVE = frozenset("""
bad terrible awful horrible worst poor poorly broke broken waste wasted disappointed disappointing
disappointment useless junk garbage trash return returned returning refund defective flimsy
stopped fails failed fail faulty damaged leaked leaking leaks itchy rash sticky greasy fake
counterfeit overpriced unhappy hate hated sucks crap mediocre weak problem problems issue issues
cheaply ripped tore torn burned burning irritation irritated smelly stinks unusable worthless
annoying wrong missing dead died scam misleading regret allergic breakout ruined
""".split())

NEGATORS = frozenset("""
not no never nothing hardly barely isn't isnt wasn't wasnt don't dont doesn't doesnt didn't didnt
won't wont can't cant cannot couldn't couldnt wouldn't wouldnt shouldn't shouldnt aren't arent
""".split())


def lexicon_sentiment(tokens: list[str]) -> tuple[float, int]:
    """Returns (score in [-1, 1], number of polar tokens). Negators in the previous 2 tokens flip."""
    total = 0
    polar = 0
    for i, t in enumerate(tokens):
        v = 1 if t in POSITIVE else (-1 if t in NEGATIVE else 0)
        if v == 0:
            continue
        if (i >= 1 and tokens[i - 1] in NEGATORS) or (i >= 2 and tokens[i - 2] in NEGATORS):
            v = -v
        total += v
        polar += 1
    if polar == 0:
        return 0.0, 0
    return float(total / np.sqrt(total * total + 4.0)), polar


# --------------------------------------------------------------------------- incentivised disclosure
INCENTIVIZED = re.compile(
    r"in exchange for (my|an|a|this|the) (honest|unbiased|fair|truthful)"
    r"|(received|got|was given|was sent|was provided|were given|were sent)( this| the| a| these)?"
    r"( [a-z]+){0,3} (for free|free of charge|at a (discount|discounted|reduced|promotional))"
    r"|(discount|discounted|free|complimentary) (product |item |sample )?(in exchange|for (my|an) (honest|unbiased))"
    r"|provided (to me )?(free |at a discount )?by the (seller|manufacturer|company|vendor)"
    r"|honest (and unbiased |unbiased )?review (in exchange|for a discount)",
)


def is_incentivized(text_lower: str) -> bool:
    return bool(INCENTIVIZED.search(text_lower))


# --------------------------------------------------------------------------- batch entry point
def text_features_batch(texts, titles, *, k: int, num_perm: int, bands: int,
                        min_shingles: int, seed: int = 1729) -> dict[str, list]:
    """Process one Arrow batch. Returns column lists for the UDF struct output."""
    n_tokens, sentiment, polar, incent, eligible, band_lists = [], [], [], [], [], []
    hash_sets = []
    for text, title in zip(texts, titles):
        toks = tokenize(text)
        body_and_title = tokenize(title) + toks if title else toks
        s, p = lexicon_sentiment(body_and_title)
        n_tokens.append(len(toks))
        sentiment.append(s)
        polar.append(p)
        incent.append(is_incentivized(" ".join(toks)))
        hs = shingle_hashes(toks, k)
        ok = len(hs) >= min_shingles
        eligible.append(ok)
        hash_sets.append(hs if ok else np.empty(0, dtype=np.uint64))
    sigs = minhash_signatures(hash_sets, num_perm, seed)
    keys = band_keys(sigs, bands)
    for i, ok in enumerate(eligible):
        band_lists.append(keys[i].tolist() if ok else None)
    return {"n_tokens": n_tokens, "sentiment": sentiment, "polar_tokens": polar,
            "incentivized": incent, "lsh_bands": band_lists}
