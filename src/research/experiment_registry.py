from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


@dataclass
class ExperimentConfig:
    experiment_id: str
    market: str
    model_type: str
    params: Dict[str, object] = field(default_factory=dict)
    line: Optional[int] = None
    calibration: Optional[str] = None
    uses_features: bool = False
    uses_counts: bool = False


def _exp_id(parts: Iterable[str]) -> str:
    return "__".join([p for p in parts if p])


def list_experiments(markets: List[str], include_heavy: bool = True) -> List[ExperimentConfig]:
    experiments: List[ExperimentConfig] = []
    for market in markets:
        experiments.append(
            ExperimentConfig(
                experiment_id=_exp_id(["baseline", market]),
                market=market,
                model_type="baseline",
            )
        )

        if market in {"GOALS", "ASSISTS", "POINTS"}:
            experiments.extend(
                [
                    ExperimentConfig(
                        experiment_id=_exp_id(["poisson_mu", market]),
                        market=market,
                        model_type="poisson_mu",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["zip_poisson", market]),
                        market=market,
                        model_type="zip_poisson",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["hurdle_poisson", market]),
                        market=market,
                        model_type="hurdle_poisson",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["comp_poisson_approx", market]),
                        market=market,
                        model_type="comp_poisson_approx",
                        uses_counts=True,
                    ),
                ]
            )
        else:
            experiments.extend(
                [
                    ExperimentConfig(
                        experiment_id=_exp_id(["negbin_mu", market]),
                        market=market,
                        model_type="negbin_mu",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["zip_negbin", market]),
                        market=market,
                        model_type="zip_negbin",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["hurdle_negbin", market]),
                        market=market,
                        model_type="hurdle_negbin",
                        uses_counts=True,
                    ),
                    ExperimentConfig(
                        experiment_id=_exp_id(["comp_poisson_approx", market]),
                        market=market,
                        model_type="comp_poisson_approx",
                        uses_counts=True,
                    ),
                ]
            )

        for calib in ["isotonic", "platt", "beta", "temp", "spline", "binned_isotonic"]:
            experiments.append(
                ExperimentConfig(
                    experiment_id=_exp_id(["calib", calib, market]),
                    market=market,
                    model_type="calibration",
                    calibration=calib,
                )
            )

        if include_heavy:
            for calib_model in ["calib_logreg_features", "calib_hgb_features"]:
                experiments.append(
                    ExperimentConfig(
                        experiment_id=_exp_id([calib_model, market]),
                        market=market,
                        model_type=calib_model,
                        uses_features=True,
                    )
                )
            for model in ["logreg_features", "poisson_reg_features", "hgb_features"]:
                experiments.append(
                    ExperimentConfig(
                        experiment_id=_exp_id([model, market]),
                        market=market,
                        model_type=model,
                        uses_features=True,
                        uses_counts=model == "poisson_reg_features",
                    )
                )

            if market in {"GOALS", "ASSISTS", "POINTS"}:
                line_candidates = [1, 2, 3]
            elif market == "SOG":
                line_candidates = [1, 2, 3, 4, 5]
            else:
                line_candidates = [1, 2, 3, 4]
            for line in line_candidates:
                experiments.append(
                    ExperimentConfig(
                        experiment_id=_exp_id(["direct_threshold_logreg", market, f"line{line}"]),
                        market=market,
                        model_type="direct_threshold_logreg",
                        line=line,
                        uses_features=True,
                    )
                )

    return experiments
