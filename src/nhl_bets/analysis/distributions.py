from nhl_bets.common.distributions import poisson_probability, nbinom_probability, calc_prob_from_line as common_calc
from nhl_bets.projections.config import ALPHAS

def calc_prob_from_line(line, mean, side, stat_type='goals'):
    """Wrapper using canonical ALPHAS from config."""
    return common_calc(line, mean, side, stat_type, alphas_dict=ALPHAS)