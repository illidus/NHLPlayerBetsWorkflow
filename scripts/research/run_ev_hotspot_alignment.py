import argparse
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingClassifier


DFS_TOKENS = ["underdog", "prizepicks", "sleeper", "pickem", "pick'em", "pick em"]
AMBIGUOUS_TOKENS = ["unknown", "tbd", "consensus"]


def _parse_ev(value):
    if pd.isna(value):
        return np.nan
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "").replace("+", "")
    try:
        return float(text) / 100.0
    except ValueError:
        return np.nan


def _normalize_book(value):
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _normalize_side(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _normalize_market(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper()


def _build_snapshot_long(prob_df, base_df, ctx_df):
    merged = prob_df.merge(
        base_df[["Player", "Team", "Pos", "TOI", "pp_toi_minutes_L20"]],
        on=["Player", "Team"],
        how="left",
    ).merge(
        ctx_df[["Player", "Team", "OppTeam", "opp_sa60", "opp_xga60"]],
        on=["Player", "Team"],
        how="left",
    )

    merged = merged.rename(
        columns={
            "Pos": "position",
            "TOI": "avg_toi_minutes_L10",
            "pp_toi_minutes_L20": "pp_toi_minutes_L20",
            "opp_sa60": "opp_sa60_L10",
            "opp_xga60": "opp_xga60_L10",
        }
    )
    merged["home_or_away"] = pd.NA

    mapping = {
        "GOALS": ("p_G_1plus", "mu_adj_G"),
        "ASSISTS": ("p_A_1plus", "mu_adj_A"),
        "POINTS": ("p_PTS_1plus", "mu_adj_PTS"),
        "SOG": ("p_SOG_1plus", "mu_adj_SOG"),
        "BLOCKS": ("p_BLK_1plus", "mu_adj_BLK"),
    }

    rows = []
    for market, (p_col, mu_col) in mapping.items():
        subset = merged.copy()
        subset["market"] = market
        subset["p_over_baseline"] = pd.to_numeric(subset[p_col], errors="coerce")
        subset["mu_used"] = pd.to_numeric(subset[mu_col], errors="coerce")
        rows.append(
            subset[
                [
                    "Player",
                    "Team",
                    "market",
                    "p_over_baseline",
                    "mu_used",
                    "avg_toi_minutes_L10",
                    "pp_toi_minutes_L20",
                    "opp_sa60_L10",
                    "opp_xga60_L10",
                    "position",
                    "home_or_away",
                    "prob_snapshot_ts",
                ]
            ]
        )
    return pd.concat(rows, ignore_index=True)


def _train_models(con, market):
    df = con.execute(
        """
        SELECT
            market,
            line,
            p_over_baseline,
            mu_used,
            avg_toi_minutes_L10,
            pp_toi_minutes_L20,
            opp_sa60_L10,
            opp_xga60_L10,
            home_or_away,
            position,
            y
        FROM fact_calibration_dataset
        WHERE market = ? AND line = 1
        """,
        [market],
    ).df()

    features = [
        "p_over_baseline",
        "mu_used",
        "avg_toi_minutes_L10",
        "pp_toi_minutes_L20",
        "opp_sa60_L10",
        "opp_xga60_L10",
        "home_or_away",
        "position",
    ]
    X = df[features].copy()
    y = df["y"].values
    X = pd.get_dummies(X, columns=["home_or_away", "position"], dummy_na=True)

    logreg = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=False)),
            ("logit", LogisticRegression(max_iter=1000, random_state=7)),
        ]
    )
    logreg.fit(X, y)

    hgb = HistGradientBoostingClassifier(random_state=7, max_depth=6)
    hgb.fit(X, y)

    return {
        "features": features,
        "columns": X.columns.tolist(),
        "logreg": logreg,
        "hgb": hgb,
    }


def _predict(models, df):
    X = df[models["features"]].copy()
    X = pd.get_dummies(X, columns=["home_or_away", "position"], dummy_na=True)
    X = X.reindex(columns=models["columns"], fill_value=0)
    p_logreg = models["logreg"].predict_proba(X)[:, 1]
    p_hgb = models["hgb"].predict_proba(X)[:, 1]
    return p_logreg, p_hgb


def _infer_book_type(book):
    name = _normalize_book(book)
    if not name:
        return "UNKNOWN", "missing_book_name"
    if any(token in name for token in AMBIGUOUS_TOKENS):
        return "UNKNOWN", "ambiguous_book_name"
    if any(token in name for token in DFS_TOKENS):
        return "DFS_FIXED_PAYOUT", "name_contains_dfs_token"
    return "SPORTSBOOK", "default_sportsbook"


def _apply_book_type(top_df, con):
    hashes = top_df["raw_payload_hash"].dropna().unique().tolist()
    vendors = top_df["source_vendor"].dropna().unique().tolist()
    if hashes:
        odds_df = con.execute(
            """
            SELECT
                raw_payload_hash,
                source_vendor,
                book_name_raw,
                book_type,
                market_type,
                line,
                side
            FROM fact_prop_odds
            WHERE raw_payload_hash IN ? AND source_vendor IN ?
            """,
            [hashes, vendors],
        ).df()
    else:
        odds_df = pd.DataFrame(
            columns=[
                "raw_payload_hash",
                "source_vendor",
                "book_name_raw",
                "book_type",
                "market_type",
                "line",
                "side",
            ]
        )

    odds_df["book_key"] = odds_df["book_name_raw"].apply(_normalize_book)
    odds_df["market_key"] = odds_df["market_type"].apply(_normalize_market)
    odds_df["side_key"] = odds_df["side"].apply(_normalize_side)
    odds_df["line_key"] = pd.to_numeric(odds_df["line"], errors="coerce").round(3)

    enriched = top_df.copy()
    enriched["row_id"] = np.arange(len(enriched))
    enriched["book_key"] = enriched["Book"].apply(_normalize_book)
    enriched["market_key"] = enriched["Market"].apply(_normalize_market)
    enriched["side_key"] = enriched["Side"].apply(_normalize_side)
    enriched["line_key"] = pd.to_numeric(enriched["Line"], errors="coerce").round(3)

    merged = enriched.merge(
        odds_df[
            [
                "raw_payload_hash",
                "source_vendor",
                "book_key",
                "market_key",
                "line_key",
                "side_key",
                "book_type",
            ]
        ],
        on=["raw_payload_hash", "source_vendor", "book_key", "market_key", "line_key", "side_key"],
        how="left",
    )

    merged = merged.sort_values(by=["row_id", "book_type"], na_position="last")
    merged = merged.drop_duplicates(subset=["row_id"], keep="first")

    merged["book_type_final"] = merged["book_type"]
    merged["book_type_source"] = np.where(merged["book_type"].notna(), "native", "inferred")
    merged["book_type_inference_rule"] = ""

    inferred_mask = merged["book_type"].isna()
    inferred = merged.loc[inferred_mask, "Book"].apply(_infer_book_type)
    merged.loc[inferred_mask, "book_type_final"] = inferred.apply(lambda x: x[0])
    merged.loc[inferred_mask, "book_type_inference_rule"] = inferred.apply(lambda x: x[1])

    merged = merged.drop(columns=["book_key", "market_key", "side_key", "line_key", "book_type", "row_id"])
    return merged


def _tail_shift(df, subset_label):
    rows = []
    for market in sorted(df["market"].dropna().unique()):
        sub = df[df["market"] == market].copy()
        if sub.empty:
            continue
        baseline = sub["p_over_baseline"]
        baseline_tail = float(np.mean(baseline >= 0.5)) if len(baseline) else np.nan
        baseline_count = int((baseline >= 0.5).sum())
        rows.append(
            {
                "subset": subset_label,
                "market": market,
                "model": "baseline",
                "n_rows": len(sub),
                "tail_count": baseline_count,
                "tail_rate": baseline_tail,
                "tail_shift_vs_baseline": 0.0,
            }
        )
        for model_col, model_name in [
            ("p_calib_logreg_features", "calib_logreg_features"),
            ("p_calib_hgb_features", "calib_hgb_features"),
        ]:
            probs = sub[model_col]
            tail_rate = float(np.mean(probs >= 0.5)) if len(probs) else np.nan
            tail_count = int((probs >= 0.5).sum())
            rows.append(
                {
                    "subset": subset_label,
                    "market": market,
                    "model": model_name,
                    "n_rows": len(sub),
                    "tail_count": tail_count,
                    "tail_rate": tail_rate,
                    "tail_shift_vs_baseline": tail_rate - baseline_tail,
                }
            )
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ev-path", default="outputs/ev_analysis/MultiBookBestBets.xlsx")
    parser.add_argument("--prob-path", default="outputs/projections/SingleGamePropProbabilities.csv")
    parser.add_argument("--base-proj-path", default="outputs/projections/BaseSingleGameProjections.csv")
    parser.add_argument("--game-context-path", default="outputs/projections/GameContext.csv")
    parser.add_argument("--duckdb-path", default="data/db/nhl_backtest.duckdb")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir or f"outputs/research/{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    ev_df = pd.read_excel(args.ev_path)
    ev_df["EV%_num"] = ev_df["EV%"].apply(_parse_ev)
    ev_df = ev_df.sort_values("EV%_num", ascending=False)
    top_df = ev_df.head(200).copy()

    con = duckdb.connect(args.duckdb_path)
    top_df = _apply_book_type(top_df, con)

    prob_df = pd.read_csv(args.prob_path)
    base_df = pd.read_csv(args.base_proj_path)
    ctx_df = pd.read_csv(args.game_context_path)

    snapshot_long = _build_snapshot_long(prob_df, base_df, ctx_df)

    model_map = {}
    for market in sorted(snapshot_long["market"].unique()):
        model_map[market] = _train_models(con, market)

    con.close()

    preds = []
    for market, models in model_map.items():
        sub = snapshot_long[snapshot_long["market"] == market].copy()
        p_logreg, p_hgb = _predict(models, sub)
        sub["p_calib_logreg_features"] = p_logreg
        sub["p_calib_hgb_features"] = p_hgb
        preds.append(sub)
    snapshot_long = pd.concat(preds, ignore_index=True)

    top_df = top_df.merge(
        snapshot_long[
            [
                "Player",
                "Team",
                "market",
                "p_over_baseline",
                "p_calib_logreg_features",
                "p_calib_hgb_features",
                "prob_snapshot_ts",
            ]
        ],
        left_on=["Player", "Team", "Market"],
        right_on=["Player", "Team", "market"],
        how="left",
    )
    top_df = top_df.drop(columns=["market"])

    top_line_mask = pd.to_numeric(top_df["Line"], errors="coerce").sub(0.5).abs() < 1e-6
    top_line_df = top_df[top_line_mask].copy()

    overall_tail = _tail_shift(snapshot_long, "overall")
    top_tail = _tail_shift(top_line_df.rename(columns={"Market": "market"}), "top200_ev_line_0p5")

    top_df.to_csv(out_dir / "top200_ev_enriched.csv", index=False)
    overall_tail.to_csv(out_dir / "tail_shift_overall.csv", index=False)
    top_tail.to_csv(out_dir / "tail_shift_top200.csv", index=False)

    book_summary = (
        top_df.groupby(["book_type_final", "book_type_source"])["Player"]
        .count()
        .reset_index(name="count")
        .sort_values(["count"], ascending=False)
    )

    report_path = out_dir / "report.md"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("# EV Hotspot Tail-Shift Alignment\n\n")
        f.write(f"- Source EV file: `{args.ev_path}`\n")
        f.write(f"- Snapshot file: `{args.prob_path}`\n")
        f.write(f"- Output timestamp (UTC): `{ts}`\n\n")
        f.write("## Book Type Coverage (Top 200 EV)\n\n")
        f.write(book_summary.to_string(index=False))
        f.write("\n\n")
        f.write("## Tail Shift (Line 0.5, Overall Snapshot)\n\n")
        f.write(overall_tail.to_string(index=False))
        f.write("\n\n")
        f.write("## Tail Shift (Line 0.5, Top 200 EV Rows Only)\n\n")
        f.write(top_tail.to_string(index=False))
        f.write("\n")


if __name__ == "__main__":
    main()
