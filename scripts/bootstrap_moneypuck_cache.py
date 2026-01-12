import argparse
import zipfile
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Bootstrap MoneyPuck cache from a zip archive.")
    parser.add_argument("--from-zip", type=str, required=True, help="Path to the zip file containing MoneyPuck data.")
    parser.add_argument("--clear-existing", action="store_true", help="Delete existing data before unpacking.")
    
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    data_root = project_root / "data" / "raw" / "moneypuck"
    
    args = parser.parse_args()
    
    zip_path = Path(args.from_zip)
    if not zip_path.exists():
        logger.error(f"Zip file not found: {zip_path}")
        exit(1)
        
    logger.info(f"Bootstrapping MoneyPuck cache from {zip_path}...")
    
    if args.clear_existing and data_root.exists():
        logger.warning(f"Clearing existing data at {data_root}")
        shutil.rmtree(data_root)
        
    data_root.mkdir(parents=True, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Basic validation: check if it looks like the right structure
            # Expecting either a root 'moneypuck' folder or contents directly.
            # We'll just unzip.
            zip_ref.extractall(data_root)
            
        logger.info(f"Extracted to {data_root}")
        
        # Verify structure roughly
        expected_lookup = data_root / "allPlayersLookup.csv"
        if not expected_lookup.exists():
            # Maybe it was inside a subfolder?
            # Check if there is a 'moneypuck' subfolder
            subfolder = data_root / "moneypuck"
            if subfolder.exists() and (subfolder / "allPlayersLookup.csv").exists():
                logger.info("Detected nested 'moneypuck' folder. Adjusting structure...")
                # Move contents up
                for item in subfolder.iterdir():
                    shutil.move(str(item), str(data_root))
                subfolder.rmdir()
                
        if not expected_lookup.exists():
            logger.warning("Warning: allPlayersLookup.csv not found. Structure might be incorrect.")
        
        # Write Manifest
        manifest_path = data_root / "_manifest.json"
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "source": "bootstrap_zip",
            "zip_file": zip_path.name,
            "status": "valid"
        }
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
            
        logger.info(f"Manifest created at {manifest_path}")
        logger.info("Bootstrap complete.")
        
    except Exception as e:
        logger.error(f"Failed to bootstrap: {e}")
        exit(1)

if __name__ == "__main__":
    main()
