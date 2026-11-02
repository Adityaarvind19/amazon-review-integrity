import json

from revint import sample
from revint.evaluate import auc, run_eval


def test_auc():
    assert auc([0.9, 0.8, 0.1, 0.2], [1, 1, 0, 0]) == 1.0
    assert auc([0.5, 0.5], [1, 0]) == 0.5
    assert auc([1, 2], [1, 1]) is None


def test_injection_eval_end_to_end(make_cfg, tmp_path):
    cfg = make_cfg("eval")
    sample.generate(cfg["paths"]["raw_dir"], "All_Beauty", n_products=300, n_users=2000, n_reviews=7000, seed=7)
    rep = run_eval(cfg, "All_Beauty", [5], campaigns_per_type=2, work_dir=str(tmp_path / "evalwork"))
    r = rep["per_seed"][0]
    assert r["labels_matched_in_fact"] == r["labels"]          # every injected review survived the pipeline
    by_type = r["review_level"]["recall_by_campaign_type"]
    assert by_type["TEMPLATE"]["combined_flag"] >= 0.9           # regression guard on the easy cases
    assert by_type["REVIEW_BOMB"]["combined_flag"] >= 0.9
    assert r["product_level"]["auc_integrity_score"] > 0.85
    assert r["review_level"]["organic_flag_rate"] < 0.05
    md = (cfg["paths"]["results_dir"] + "/eval/eval_report.md")
    assert "Recall by campaign type" in open(md).read()
    json.loads(open(cfg["paths"]["results_dir"] + "/eval/eval_report.json").read())
