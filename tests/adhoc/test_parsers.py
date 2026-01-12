from odds_archive.parsers import build_registry

def test():
    registry = build_registry()
    
    text = "Carter Verhaeghe over 0.5 points (-125)"
    print(f"Testing: '{text}'")
    results = registry.parse(text)
    for r in results:
        print(r)

    text2 = "Dallas Stars -1.5 (+150)"
    print(f"\nTesting: '{text2}'")
    results2 = registry.parse(text2)
    for r in results2:
        print(r)

if __name__ == "__main__":
    test()

