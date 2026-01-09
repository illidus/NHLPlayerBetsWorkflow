# Provider Evaluation Template

## 1. Executive Summary
- **Provider Name:** [Name]
- **Website:** [URL]
- **API Documentation:** [URL]
- **Cost Model:** [e.g., Pay-per-call, Subscription, Free Tier]
- **Recommendation:** [GO / NO-GO / HOLD]

## 2. Coverage Analysis

### 2.1 League & Event Coverage
- [ ] NHL Regular Season coverage?
- [ ] Historical depth (How far back?): [e.g., 2021-Present]
- [ ] Pre-game vs. Live odds distinction?

### 2.2 Market Coverage (Player Props)
Tick supported markets:
- [ ] Goals (`Anytime Goalscorer`)
- [ ] Assists (`Player Assists`)
- [ ] Points (`Player Points`)
- [ ] Shots on Goal (`Player Shots on Goal`)
- [ ] Blocked Shots (`Player Blocked Shots`)
- [ ] Power Play Points
- [ ] Goalie Saves
- [ ] Goalie Goals Against

### 2.3 Bookmaker Coverage
List key US/Canada books supported (e.g., DraftKings, FanDuel, BetMGM, Bet365, Pinnacle, PlayNow):
- ...

## 3. Data Quality & format

### 3.1 Timestamps
- [ ] Odds capture timestamps provided? (Critical for ROI backtesting)
- [ ] Game start times provided?
- [ ] Timezone format (UTC/ISO8601)?

### 3.2 Player Identity
- [ ] Internal stable Player IDs?
- [ ] Standardized names (e.g., "Connor McDavid" vs "C. McDavid")?
- [ ] Team association provided?

### 3.3 Odds Format
- [ ] American (-110)
- [ ] Decimal (1.91)
- [ ] Implied Probability

## 4. Technical Integration

### 4.1 Rate Limits
- Requests per minute/day:
- Cost efficiency calculation:

### 4.2 API Structure
- JSON structure complexity: [Low/Med/High]
- Batch fetching support? (e.g., "All props for Game X" or "All props for Date Y")

## 5. Sample Payload (Snippet)
```json
{
  "key": "value"
}
```

## 6. Verdict & Scoring
| Criterion | Weight | Score (1-5) | Notes |
|Str|---|---|---|
| Historical Depth | 30% | | |
| Prop Variety | 30% | | |
| Timestamp Precision | 20% | | |
| Cost | 20% | | |
| **Total** | | **0.0** | |

## 7. Free Tier Validation Scope
### Can Validate:
- Plumbing: Connectivity, JSON parsing, Schema Normalization.
- Coverage (Current): Can see what markets/books are available *right now*.
- Timestamps: Can verify timestamp formats for *live* odds.
- Identities: Can inspect player name formats and team abbrs.

### Cannot Validate:
- Historical Depth: Free tier usually blocks historical endpoints (401 Unauthorized).
- Historical Stability: Cannot verify if player IDs change over seasons.
- Backfill Cost: Cannot accurately estimate historical backfill API units without paid key.

