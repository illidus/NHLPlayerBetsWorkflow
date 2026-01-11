import shutil
import hashlib
import sys
import argparse
import os

SOURCE = "config/production_profile.json"
DEST = "config/production_experiment_b.json"

def get_hash(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description="Sync or check profile aliases.")
    parser.add_argument("--check", action="store_true", help="Verify aliases match without copying.")
    args = parser.parse_args()

    # Ensure source exists
    if not os.path.exists(SOURCE):
        print(f"Error: Source profile {SOURCE} missing.")
        sys.exit(1)

    if args.check:
        src_hash = get_hash(SOURCE)
        dest_hash = get_hash(DEST)
        
        if dest_hash is None:
            print(f"FAIL: Alias {DEST} missing.")
            sys.exit(1)
            
        if src_hash != dest_hash:
            print(f"FAIL: Alias {DEST} does not match source {SOURCE}.")
            sys.exit(1)
            
        print("SUCCESS: Profile aliases are in sync.")
        sys.exit(0)
    else:
        # Sync Mode
        print(f"Syncing {SOURCE} -> {DEST}...")
        shutil.copy2(SOURCE, DEST)
        print("Done.")

if __name__ == "__main__":
    main()
