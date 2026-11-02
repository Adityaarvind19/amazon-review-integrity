import numpy as np

from revint import textfeat as tf


def test_tokenize_strips_html_and_entities():
    assert tf.tokenize("Great<br /><br />product &amp; FAST shipping!") == ["great", "product", "fast", "shipping"]
    assert tf.tokenize(None) == []
    assert tf.tokenize("Don’t buy") == ["don't", "buy"]


def test_sentiment_negation_flips_polarity():
    pos, _ = tf.lexicon_sentiment(tf.tokenize("This is good and I love it"))
    neg, _ = tf.lexicon_sentiment(tf.tokenize("This is not good, it broke"))
    flip, polar = tf.lexicon_sentiment(tf.tokenize("not bad at all"))
    assert pos > 0.5 and neg < -0.5 and flip > 0 and polar == 1
    assert tf.lexicon_sentiment([]) == (0.0, 0)


def test_incentivized_disclosure_regex():
    yes = ["I received this product at a discount in exchange for my honest review",
           "i was given this item for free in exchange for my unbiased review",
           "this was provided by the manufacturer"]
    no = ["I paid full price and I am honest when I say it is good", "free shipping was nice"]
    assert all(tf.is_incentivized(" ".join(tf.tokenize(t))) for t in yes)
    assert not any(tf.is_incentivized(" ".join(tf.tokenize(t))) for t in no)


def test_minhash_identical_docs_share_all_bands_and_estimate_jaccard():
    a = tf.tokenize("the quick brown fox jumps over the lazy dog while the cat sleeps on the warm mat today")
    b = list(a)
    b[3] = "wolf"
    c = tf.tokenize("completely different words describing an unrelated kitchen blender that broke after a week")
    hs = [tf.shingle_hashes(x, 3) for x in (a, a, b, c)]
    sig = tf.minhash_signatures(hs, 128)
    keys = tf.band_keys(sig, 32)
    assert (keys[0] == keys[1]).all()
    est_ab = float((sig[0] == sig[2]).mean())
    assert abs(est_ab - tf.jaccard(a, b, 3)) < 0.2
    assert (keys[0] == keys[3]).sum() == 0


def test_minhash_handles_empty_docs():
    sig = tf.minhash_signatures([np.empty(0, dtype=np.uint64)], 16)
    assert sig.shape == (1, 16) and (sig == tf.MERSENNE_P).all()


def test_batch_marks_short_texts_ineligible():
    out = tf.text_features_batch(["Great product", " ".join(f"w{i}" for i in range(30))], [None, "Title"], k=3, num_perm=16, bands=4,
                                 min_shingles=8)
    assert out["lsh_bands"][0] is None and len(out["lsh_bands"][1]) == 4
