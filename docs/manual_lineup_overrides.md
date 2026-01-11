# Manual Lineup Overrides: Specification and Usage Guide

This file documents how to use `data/overrides/manual_lineup_overrides.csv` to inject real-time adjustments into the NHL player prop model. This includes promotions to top lines, power play units, or projected TOI updates due to coach reports or lineup changes.

---

## ✅ CSV Schema

Each row should have the following fields:

| Column Name     | Type    | Required | Description |
|-----------------|---------|----------|-------------|
| `player_name`   | string  | ✅       | Full name as it appears in projections (case-sensitive match). |
| `projected_toi` | float   | ✅       | The TOI you want the model to use instead of the L20 rolling average. |
| `pp_unit`       | int     | ⛔️ (optional) | Power play unit assignment: `1` for PP1, `2` for PP2. Triggers PP heuristic if player had no prior PP time. |
| `line_number`   | int     | ⛔️ (optional) | Informational field (not used by model directly). Useful for audits or future line-aware features. |


---

## 🔁 Integration Behavior

- Overrides are merged in `produce_game_context.py` using player name as key.
- `projected_toi` replaces the baseline TOI before it’s passed to `single_game_model.py`.
- If `pp_unit` is provided and the player has little or no PP history, the model applies a **heuristic PP ratio floor** (e.g. 50%) to simulate PP deployment.
- The final projections include a `is_manual_toi` column = `1` for any player with an override, `0` otherwise.


---

## 🔍 Example Row

```csv
player_name,projected_toi,pp_unit,line_number
Connor Bedard,19.5,1,1
```

> ⬆️ This boosts Bedard to 19.5 minutes and treats him as on PP1, increasing his point/scoring projection even if he had no PP1 minutes in L20.


---

## 🧪 Testing Checklist

- [ ] File is located at: `data/overrides/manual_lineup_overrides.csv`
- [ ] Column headers match spec exactly (no typos)
- [ ] All `player_name` values match projected players (check for accents, typos)
- [ ] Run full pipeline via `single_game_probs.py` and inspect `is_manual_toi` column
- [ ] Compare final projections to confirm override was applied


---

## 💡 Use Cases

- Player promoted to Line 1 or PP1 during morning skate
- Breaking lineup changes (e.g., Twitter, beat reporters)
- Coach quotes on ice time changes
- Filling in gaps where L20 average TOI doesn’t reflect recent usage (e.g., return from injury)


---

## 📌 Notes

- If a player is not present in the override file, no changes are applied.
- If `pp_unit` is specified but no projected_toi is provided, no change occurs.
- If both `pp_unit` and `projected_toi` are given, both effects apply.


---

## 🛠 Future Extensions (Optional)

- Allow override of projected PP TOI explicitly
- Integrate `line_number` into multiplier logic (e.g., Line 1 = +10%)
- Support opponent-aware adjustments

