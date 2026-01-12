from odds_archive import parsers

def test_parsers():
    registry = parsers.build_registry()
    
    test_texts = [
        "Connor McDavid over 1.5 points -120 at FanDuel",
        "Auston Matthews shots on goal under 3.5 (+105)",
        "Nathan MacKinnon assists over 0.5 -150",
        "Leon Draisaitl (points over 1.5) -110",
        "Cale Makar goals over 0.5 +300 at DraftKings",
    ]
    
    for text in test_texts:
        candidates = registry.parse(text)
        print(f"Text: {text}")
        for c in candidates:
            print(f"  Parsed: {c.player_name_raw} | {c.market_raw} | {c.side} | {c.line} | {c.odds} | {c.bookmaker}")
        if not candidates:
            print("  No candidates found")

if __name__ == "__main__":
    test_parsers()
