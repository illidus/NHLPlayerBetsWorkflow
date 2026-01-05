import re
from collections import defaultdict

class Bet:
    def __init__(self, game_slug, market_raw, player_raw, odds_decimal, raw_line, 
                 stat_type, line_value=None, side=None, threshold_k=None, game_date=None):
        self.game_slug = game_slug
        self.game_date = game_date
        self.market_raw = market_raw
        self.player_raw = player_raw
        self.odds_decimal = float(odds_decimal)
        self.raw_line = raw_line
        
        self.stat_type = stat_type # 'goals', 'assists', 'points', 'sog'
        self.line_value = float(line_value) if line_value is not None else None
        self.side = side # 'over', 'under'
        self.threshold_k = int(threshold_k) if threshold_k is not None else None
        
        self.supported = True
        self.reason = ""
        
        # Computed later
        self.implied_prob_raw = 0.0
        self.implied_prob_novig = 0.0
        self.model_mean = 0.0
        self.model_prob = 0.0
        self.ev = 0.0
        self.edge = 0.0
        
        # Matching info
        self.player_matched = None
        self.team_matched = None
        self.match_score = 0.0
        self.audit = {} # Stores calculation steps

def parse_bets(df):
    """
    Parses the raw DataFrame into a list of Bet objects.
    Groups 'Total' markets into pairs to infer sides.
    """
    bets = []
    
    # Temporary storage for grouping Two-Sided markets
    # Key: (game, player_norm, stat, line)
    # Value: list of (index, row_data)
    grouped_markets = defaultdict(list)
    
    goal_regex = re.compile(r"Player\s+(?P<k>\d+)\+\s+Goals", re.IGNORECASE)
    total_regex = re.compile(r"^(?P<name>.+?)\s+Total\s+(?P<stat>Assists|Points|Shots On Goal|Blocks|Blocked Shots)\s+(?P<line>[0-9]+(?:\.[0-9]+)?)$", re.IGNORECASE)
    
    # Pass 1: Categorize and Standardize
    for idx, row in df.iterrows():
        market = str(row['Market']).strip()
        player = str(row['Player']).strip()
        game = str(row['Game']).strip()
        # Handle Game_Date if it exists, else None
        game_date = str(row['Game_Date']).strip() if 'Game_Date' in row and pd.notna(row['Game_Date']) else None
        odds = row['Odds_1']
        
        # Skip invalid odds
        if pd.isna(odds) or odds == '':
            continue
            
        # 1. Goal Scorer (Unsupported)
        if 'goal scorer' in market.lower():
            b = Bet(game, market, player, odds, row['Raw_Line'], 'goals', game_date=game_date)
            b.supported = False
            b.reason = "GoalScorer market (First/Last) is unsupported."
            bets.append(b)
            continue
            
        # 2. X+ Goals (Single Sided)
        m_goals = goal_regex.search(market)
        if m_goals:
            k = m_goals.group('k')
            b = Bet(game, market, player, odds, row['Raw_Line'], 'goals', 
                    threshold_k=k, side='over', game_date=game_date)
            # Line is implicitly k-0.5 technically for 'over', but we use k for P(X>=k)
            b.line_value = float(k) - 0.5 
            bets.append(b)
            continue
            
        # 3. Total Stats (Potentially Two Sided)
        m_total = total_regex.search(market)
        if m_total:
            name_extracted = m_total.group('name')
            stat_str = m_total.group('stat').lower()
            line_val = float(m_total.group('line'))
            
            stat_map = {
                'assists': 'assists',
                'points': 'points',
                'shots on goal': 'sog',
                'blocks': 'blocks',
                'blocked shots': 'blocks'
            }
            stat_type = stat_map.get(stat_str, 'unknown')
            
            # Key for grouping: (Game, Name, Stat, Line)
            # We use the extracted name because 'Player' column might be the line value
            key = (game, name_extracted.lower(), stat_type, line_val)
            
            # Store raw data to create Bet objects later after grouping
            grouped_markets[key].append({
                'game': game,
                'game_date': game_date,
                'market': market,
                'player': name_extracted, # Use name from market
                'odds': odds,
                'raw_line': row['Raw_Line'],
                'stat': stat_type,
                'line': line_val
            })
            continue
            
        # 4. Unknown/Unsupported
        b = Bet(game, market, player, odds, row['Raw_Line'], 'unknown', game_date=game_date)
        b.supported = False
        b.reason = f"Unsupported market type: {market}"
        bets.append(b)

    # Pass 2: Process Groups (Infer Sides)
    # We delay this to the main loop or a separate step because we need the MODEL to infer sides.
    # Actually, we can create the Bet objects here with side=None, and mark them as "Needs Inference".
    
    for key, items in grouped_markets.items():
        if len(items) != 2:
            # If not exactly 2, we can't reliably infer pair. 
            # Treat as supported but separate (maybe single sided?).
            # But usually it's an error or just one side offered.
            for item in items:
                b = Bet(item['game'], item['market'], item['player'], item['odds'], 
                        item['raw_line'], item['stat'], line_value=item['line'], game_date=item.get('game_date'))
                b.supported = False
                b.reason = f"Found {len(items)} lines for this group; expected 2 for O/U inference."
                bets.append(b)
        else:
            # We have a pair. Create 2 bets.
            # We will flag them as needing side inference.
            # We link them to each other to calculate vig-free probs together.
            
            item1, item2 = items[0], items[1]
            
            b1 = Bet(item1['game'], item1['market'], item1['player'], item1['odds'], 
                     item1['raw_line'], item1['stat'], line_value=item1['line'], game_date=item1.get('game_date'))
            b2 = Bet(item2['game'], item2['market'], item2['player'], item2['odds'], 
                     item2['raw_line'], item2['stat'], line_value=item2['line'], game_date=item2.get('game_date'))
            
            # Link them manually (Python dynamic attr)
            b1.pair_bet = b2
            b2.pair_bet = b1
            b1.requires_inference = True
            b2.requires_inference = True
            
            bets.append(b1)
            bets.append(b2)
            
    return bets

import pandas as pd
