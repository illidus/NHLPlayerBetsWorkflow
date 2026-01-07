# Unabated UI Reconciliation Report

**Generated at (UTC):** 2026-01-07T05:55:56.169985+00:00

## Example 1

### Inputs
```json
{
  "label": "Sebastian Aho SOG Over 2.5 (Novig)",
  "vendor_event_id": 104502,
  "vendor_person_id": 44247,
  "player_name": "Sebastian Aho",
  "market": "SOG",
  "bet_type_id": 86,
  "line": 2.5,
  "side": "OVER",
  "book_name": "Novig",
  "market_source_id": 89,
  "expected_american_odds": 108
}
```

### Raw Matches
|   eventId |   personId |   betTypeId | eventStart          | eventName                                  | sideKey      | mappedSide   |   marketSourceId | bookName   | bookType   |   points |   americanPrice |   price |   sourcePrice |   sourceFormat |
|----------:|-----------:|------------:|:--------------------|:-------------------------------------------|:-------------|:-------------|-----------------:|:-----------|:-----------|---------:|----------------:|--------:|--------------:|---------------:|
|    104502 |      44247 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si1:pid44247 | UNDER        |               89 | Novig      | SPORTSBOOK |      2.5 |            -141 |    -141 |         0.585 |              4 |
|    104502 |      44247 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si0:pid44247 | OVER         |               89 | Novig      | SPORTSBOOK |      2.5 |             108 |     108 |         0.481 |              4 |

### DB Matches
|   vendor_event_id |   vendor_person_id | event_start_time_utc   | home_team   | away_team   | player_name_raw   | market_type   |   line | side   | book_name_raw   | book_type   |   odds_american |   odds_decimal |   vendor_market_source_id |   vendor_bet_type_id | vendor_outcome_key   |   vendor_price_raw | vendor_price_format   | raw_payload_hash                                                 | capture_ts_utc             |
|------------------:|-------------------:|:-----------------------|:------------|:------------|:------------------|:--------------|-------:|:-------|:----------------|:------------|----------------:|---------------:|--------------------------:|---------------------:|:---------------------|-------------------:|:----------------------|:-----------------------------------------------------------------|:---------------------------|
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |             108 |        2.08    |                        89 |                   86 | si0:pid44247         |                108 | american              | fbd93945e6e27c7f0318be3dab868041e139e2ff2e5a573408ffb19cd066da50 | 2026-01-07 04:51:43.618625 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |             108 |        2.08    |                        89 |                   86 | si0:pid44247         |                108 | american              | 06c387713a093b20d85cc2122dce28f9f9a7b0997ef63757f0118cb2b2c2d656 | 2026-01-07 04:50:27.239877 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |            -141 |        1.70922 |                        89 |                   86 | si1:pid44247         |               -141 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:18:07.674732 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |            -141 |        1.70922 |                        89 |                   86 | si1:pid44247         |               -141 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:17:15.548436 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |            -141 |        1.70922 |                        89 |                   86 | si1:pid44247         |               -141 | american              | 5216faf95247501788c1bc04e8f7ecc47785dac2d37abd5255e14f16ce4d0e69 | 2026-01-07 04:16:08.830828 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |            -141 |        1.70922 |                        89 |                   86 | si1:pid44247         |               -141 | american              | 25e506617f395b7b773a6ace951fffe53f0cdc40dc178004ef9bfbb46b86a119 | 2026-01-07 03:44:41.003806 |
|            104502 |              44247 | 2026-01-07 00:00:00    | CAR         | DAL         | Sebastian Aho     | SOG           |    2.5 | OVER   | Novig           | SPORTSBOOK  |            -141 |        1.70922 |                        89 |                   86 | si1:pid44247         |               -141 | american              | 4f3da16fc48c40dcf577dc0edd5a05883daeff84d3d20328ae3acf2353722560 | 2026-01-07 01:58:58.108081 |

### Checks
| check                 | expected            | actual              | pass   |
|:----------------------|:--------------------|:--------------------|:-------|
| american_price_match  | 108                 | [108]               | True   |
| side_mapping_match    | OVER                | ['UNDER', 'OVER']   | True   |
| decimal_from_american | 1.7092              | 1.7092              | True   |
| event_start_time_utc  | 2026-01-07T00:00:00 | 2026-01-07T00:00:00 | True   |

## Example 2

### Inputs
```json
{
  "label": "Wyatt Johnston Assists Under 0.5 (Underdog Fantasy)",
  "vendor_event_id": 104502,
  "vendor_person_id": 242424,
  "player_name": "Wyatt Johnston",
  "market": "ASSISTS",
  "bet_type_id": 73,
  "line": 0.5,
  "side": "UNDER",
  "book_name": "Underdog Fantasy",
  "market_source_id": 73,
  "expected_american_odds": 151
}
```

### Raw Matches
|   eventId |   personId |   betTypeId | eventStart          | eventName                                  | sideKey       | mappedSide   |   marketSourceId | bookName         | bookType         |   points |   americanPrice |   price |   sourcePrice |   sourceFormat |
|----------:|-----------:|------------:|:--------------------|:-------------------------------------------|:--------------|:-------------|-----------------:|:-----------------|:-----------------|---------:|----------------:|--------:|--------------:|---------------:|
|    104502 |     242424 |          73 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si1:pid242424 | UNDER        |               73 | Underdog Fantasy | DFS_FIXED_PAYOUT |      0.5 |             151 |     151 |           151 |              1 |
|    104502 |     242424 |          73 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si0:pid242424 | OVER         |               73 | Underdog Fantasy | DFS_FIXED_PAYOUT |      0.5 |            -189 |    -189 |          -189 |              1 |

### DB Matches
|   vendor_event_id |   vendor_person_id | event_start_time_utc   | home_team   | away_team   | player_name_raw   | market_type   |   line | side   | book_name_raw    | book_type        |   odds_american |   odds_decimal |   vendor_market_source_id |   vendor_bet_type_id | vendor_outcome_key   |   vendor_price_raw | vendor_price_format   | raw_payload_hash                                                 | capture_ts_utc             |
|------------------:|-------------------:|:-----------------------|:------------|:------------|:------------------|:--------------|-------:|:-------|:-----------------|:-----------------|----------------:|---------------:|--------------------------:|---------------------:|:---------------------|-------------------:|:----------------------|:-----------------------------------------------------------------|:---------------------------|
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | ASSISTS       |    0.5 | UNDER  | Underdog Fantasy | DFS_FIXED_PAYOUT |            -189 |         1.5291 |                        73 |                   73 | si0:pid242424        |               -189 | american              | 25e506617f395b7b773a6ace951fffe53f0cdc40dc178004ef9bfbb46b86a119 | 2026-01-07 03:44:41.003806 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | ASSISTS       |    0.5 | UNDER  | Underdog Fantasy | DFS_FIXED_PAYOUT |            -189 |         1.5291 |                        73 |                   73 | si0:pid242424        |               -189 | american              | 4f3da16fc48c40dcf577dc0edd5a05883daeff84d3d20328ae3acf2353722560 | 2026-01-07 01:58:58.108081 |

### Checks
| check                 | expected            | actual              | pass   |
|:----------------------|:--------------------|:--------------------|:-------|
| american_price_match  | 151                 | [151]               | True   |
| side_mapping_match    | UNDER               | ['UNDER', 'OVER']   | True   |
| decimal_from_american | 2.51                | 2.51                | True   |
| event_start_time_utc  | 2026-01-07T00:00:00 | 2026-01-07T00:00:00 | True   |

