# Unabated UI Reconciliation Report

**Generated at (UTC):** 2026-01-07T05:42:15.960436+00:00

## Example 1

### Inputs
```json
{
  "label": "Wyatt Johnston SOG Over 1.5 (FanDuel)",
  "vendor_event_id": 104502,
  "vendor_person_id": 242424,
  "player_name": "Wyatt Johnston",
  "market": "SOG",
  "bet_type_id": 86,
  "line": 1.5,
  "side": "OVER",
  "book_name": "FanDuel",
  "market_source_id": 2,
  "expected_american_odds": -188
}
```

### Raw Matches
|   eventId |   personId |   betTypeId | eventStart          | eventName                                  | sideKey       | mappedSide   |   marketSourceId | bookName   |   points |   americanPrice |   price |   sourcePrice |   sourceFormat |
|----------:|-----------:|------------:|:--------------------|:-------------------------------------------|:--------------|:-------------|-----------------:|:-----------|---------:|----------------:|--------:|--------------:|---------------:|
|    104502 |     242424 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si1:pid242424 | UNDER        |                2 | FanDuel    |      1.5 |             142 |     142 |           142 |              1 |
|    104502 |     242424 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si0:pid242424 | OVER         |                2 | FanDuel    |      1.5 |            -188 |    -188 |          -188 |              1 |

### DB Matches
|   vendor_event_id |   vendor_person_id | event_start_time_utc   | home_team   | away_team   | player_name_raw   | market_type   |   line | side   | book_name_raw   |   odds_american |   odds_decimal |   vendor_market_source_id |   vendor_bet_type_id | vendor_outcome_key   |   vendor_price_raw | vendor_price_format   | raw_payload_hash                                                 | capture_ts_utc             |
|------------------:|-------------------:|:-----------------------|:------------|:------------|:------------------|:--------------|-------:|:-------|:----------------|----------------:|---------------:|--------------------------:|---------------------:|:---------------------|-------------------:|:----------------------|:-----------------------------------------------------------------|:---------------------------|
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | fbd93945e6e27c7f0318be3dab868041e139e2ff2e5a573408ffb19cd066da50 | 2026-01-07 04:51:43.618625 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 06c387713a093b20d85cc2122dce28f9f9a7b0997ef63757f0118cb2b2c2d656 | 2026-01-07 04:50:27.239877 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:18:07.674732 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:17:15.548436 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 5216faf95247501788c1bc04e8f7ecc47785dac2d37abd5255e14f16ce4d0e69 | 2026-01-07 04:16:08.830828 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 25e506617f395b7b773a6ace951fffe53f0cdc40dc178004ef9bfbb46b86a119 | 2026-01-07 03:44:41.003806 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | OVER   | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 4f3da16fc48c40dcf577dc0edd5a05883daeff84d3d20328ae3acf2353722560 | 2026-01-07 01:58:58.108081 |

### Checks
| check                 | expected            | actual              | pass   |
|:----------------------|:--------------------|:--------------------|:-------|
| american_price_match  | -188                | [-188]              | True   |
| side_mapping_match    | OVER                | ['UNDER', 'OVER']   | True   |
| decimal_from_american | 2.42                | 2.42                | True   |
| event_start_time_utc  | 2026-01-07T00:00:00 | 2026-01-07T00:00:00 | True   |

## Example 2

### Inputs
```json
{
  "label": "Wyatt Johnston SOG Under 1.5 (FanDuel)",
  "vendor_event_id": 104502,
  "vendor_person_id": 242424,
  "player_name": "Wyatt Johnston",
  "market": "SOG",
  "bet_type_id": 86,
  "line": 1.5,
  "side": "UNDER",
  "book_name": "FanDuel",
  "market_source_id": 2,
  "expected_american_odds": 142
}
```

### Raw Matches
|   eventId |   personId |   betTypeId | eventStart          | eventName                                  | sideKey       | mappedSide   |   marketSourceId | bookName   |   points |   americanPrice |   price |   sourcePrice |   sourceFormat |
|----------:|-----------:|------------:|:--------------------|:-------------------------------------------|:--------------|:-------------|-----------------:|:-----------|---------:|----------------:|--------:|--------------:|---------------:|
|    104502 |     242424 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si1:pid242424 | UNDER        |                2 | FanDuel    |      1.5 |             142 |     142 |           142 |              1 |
|    104502 |     242424 |          86 | 2026-01-07T00:00:00 | Stars Dallas DAL @ Hurricanes Carolina CAR | si0:pid242424 | OVER         |                2 | FanDuel    |      1.5 |            -188 |    -188 |          -188 |              1 |

### DB Matches
|   vendor_event_id |   vendor_person_id | event_start_time_utc   | home_team   | away_team   | player_name_raw   | market_type   |   line | side   | book_name_raw   |   odds_american |   odds_decimal |   vendor_market_source_id |   vendor_bet_type_id | vendor_outcome_key   |   vendor_price_raw | vendor_price_format   | raw_payload_hash                                                 | capture_ts_utc             |
|------------------:|-------------------:|:-----------------------|:------------|:------------|:------------------|:--------------|-------:|:-------|:----------------|----------------:|---------------:|--------------------------:|---------------------:|:---------------------|-------------------:|:----------------------|:-----------------------------------------------------------------|:---------------------------|
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | fbd93945e6e27c7f0318be3dab868041e139e2ff2e5a573408ffb19cd066da50 | 2026-01-07 04:51:43.618625 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |             142 |        2.42    |                         2 |                   86 | si1:pid242424        |                142 | american              | 06c387713a093b20d85cc2122dce28f9f9a7b0997ef63757f0118cb2b2c2d656 | 2026-01-07 04:50:27.239877 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:18:07.674732 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 89fd2331f6733991b1652eb434d5fd17fc02e72b5c78bb93e5a2167e4355c9e7 | 2026-01-07 04:17:15.548436 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 5216faf95247501788c1bc04e8f7ecc47785dac2d37abd5255e14f16ce4d0e69 | 2026-01-07 04:16:08.830828 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 25e506617f395b7b773a6ace951fffe53f0cdc40dc178004ef9bfbb46b86a119 | 2026-01-07 03:44:41.003806 |
|            104502 |             242424 | 2026-01-07 00:00:00    | CAR         | DAL         | Wyatt Johnston    | SOG           |    1.5 | UNDER  | FanDuel         |            -188 |        1.53191 |                         2 |                   86 | si0:pid242424        |               -188 | american              | 4f3da16fc48c40dcf577dc0edd5a05883daeff84d3d20328ae3acf2353722560 | 2026-01-07 01:58:58.108081 |

### Checks
| check                 | expected            | actual              | pass   |
|:----------------------|:--------------------|:--------------------|:-------|
| american_price_match  | 142                 | [142]               | True   |
| side_mapping_match    | UNDER               | ['UNDER', 'OVER']   | True   |
| decimal_from_american | 2.42                | 2.42                | True   |
| event_start_time_utc  | 2026-01-07T00:00:00 | 2026-01-07T00:00:00 | True   |

