import sys
import time
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--start-maximized')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def get_game_links(driver, main_url):
    print(f"Navigating to {main_url}...")
    driver.get(main_url)
    try:
        # Wait for the event list to load
        try:
            WebDriverWait(driver, 10).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, "div[class*='loadingCurtain']"))
            )
        except:
            pass 

        wait = WebDriverWait(driver, 30)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/sports/event/']"))
        )
        
        # Aggressive Scrolling & "Show More" clicking
        print("Scrolling to load all games...")
        for i in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Check for "Show more" button on the matches page
            try:
                xpath = "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more')]"
                load_more_buttons = driver.find_elements(By.XPATH, xpath)
                for btn in load_more_buttons:
                    if btn.is_displayed():
                        print("Found 'Show more' button on matches list, clicking...")
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(2)
            except Exception:
                pass

        driver.execute_script("window.scrollTo(0, 0);")

        # Extract all hrefs
        elements = driver.find_elements(By.CSS_SELECTOR, "a[href*='/sports/event/']")
        print(f"Total event links found (raw): {len(elements)}")
        
        links = []
        for elem in elements:
            href = elem.get_attribute('href')
            if href:
                # We filter for NHL specifically and exclude 'builder' links
                if "hockey/north-america/nhl" in href and "marketType=builder" not in href:
                    if href not in links:
                        links.append(href)
                else:
                    # Optional: Log rejected links if they seem relevant
                    if "hockey" in href and "nhl" not in href:
                        print(f"Skipping non-NHL hockey link: {href}")
                    elif "marketType=builder" in href:
                        # print(f"Skipping builder link: {href}")
                        pass

        print(f"Found {len(links)} unique NHL games after filtering.")
        
        if len(links) == 0:
            print("No links found. Saving page source to debug_page.html...")
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
        return links
    except TimeoutException:
        print("Timeout waiting for game links to load.")
        with open("debug_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        return []

def safe_click(driver, element):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.5)
        element.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False

def expand_all(driver):
    """
    Clicks 'Load More' buttons and expands collapsed dropdowns.
    """
    # 1. Handle "Load More" / "Show more" buttons
    while True:
        try:
            # Search for buttons with text containing "Load More" or "Show more"
            xpath = "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more')]"
            load_more_buttons = driver.find_elements(By.XPATH, xpath)
            
            clicked_any = False
            for btn in load_more_buttons:
                if btn.is_displayed():
                    # print("Clicking 'Load More' / 'Show more'...")
                    if safe_click(driver, btn):
                        clicked_any = True
                        time.sleep(0.5) 
            
            if not clicked_any:
                break
        except Exception as e:
            print(f"Error handling Load More: {e}")
            break

    # 2. Handle Collapsed Dropdowns
    try:
        dropdowns = driver.find_elements(By.XPATH, "//div[@title='Expand or collapse' and contains(@class, 'Collapsed')]")
        for drop in dropdowns:
            if drop.is_displayed():
                # print("Expanding dropdown...")
                safe_click(driver, drop)
                time.sleep(0.1)
    except Exception as e:
        print(f"Error handling dropdowns: {e}")

def parse_goal_scorer_market(lines, game_id, market_title):
    data = []
    
    home_idx = -1
    away_idx = -1
    
    # Identify blocks
    for i, line in enumerate(lines):
        line_clean = line.strip()
        if line_clean == "Home":
            home_idx = i
        elif line_clean == "Away":
            away_idx = i
            
    blocks = []
    if home_idx != -1:
        end = away_idx if away_idx != -1 else len(lines)
        blocks.append(("Home", lines[home_idx+1:end]))
    if away_idx != -1:
        blocks.append(("Away", lines[away_idx+1:]))
        
    for team_name, block_lines in blocks:
        players = []
        odds = []
        capture_mode = "players" 
        
        for line in block_lines:
            line = line.strip()
            if not line: continue
            if line in ["Show more", "Show less", "Load More...", "First Goalscorer", "Last Goalscorer"]:
                if line == "First Goalscorer":
                    capture_mode = "odds"
                continue
            
            # Simple heuristic: Odds look like numbers
            if re.match(r"^\d+\.\d{2}$", line):
                if capture_mode == "odds":
                    odds.append(line)
            else:
                if capture_mode == "players":
                    # Assume player name if length is reasonable
                    if len(line) > 2:
                        players.append(line)
        
        # Expected: 2 odds per player (First, Last)
        # However, sometimes only 1 odd might be available or different market type?
        # The prompt examples show First and Last columns.
        
        if len(odds) == len(players) * 2:
            for i, player in enumerate(players):
                first_odds = odds[i*2]
                last_odds = odds[i*2+1]
                data.append({
                    "Game": game_id,
                    "Market": market_title,
                    "Sub_Header": team_name,
                    "Player": player,
                    "Odds_1": first_odds, # First Goalscorer
                    "Odds_2": last_odds,  # Last Goalscorer
                    "Raw_Line": f"{player} {first_odds} {last_odds}"
                })
        elif len(odds) == len(players):
             # Maybe only one column?
             for i, player in enumerate(players):
                data.append({
                    "Game": game_id,
                    "Market": market_title,
                    "Sub_Header": team_name,
                    "Player": player,
                    "Odds_1": odds[i],
                    "Odds_2": None,
                    "Raw_Line": f"{player} {odds[i]}"
                })
        else:
            # Fallback for mismatched counts: Zip as far as possible
            # print(f"Mismatch in Goal Scorer for {team_name}: {len(players)} players, {len(odds)} odds")
            pass
            
    return data

def parse_generic_market(lines, game_id, market_title):
    data = []
    current_player = None
    
    for line in lines:
        line = line.strip()
        if not line or line in ["Show more", "Show less", "Load More...", market_title]:
            continue
        
        # Check if it's a Team Header (Home/Away or Actual Team Names)
        # Hard to distinguish Team Name from Player Name generically without list of teams.
        # But usually Odds follow Player.
        
        if re.match(r"^\d+\.\d{2}$", line):
            if current_player:
                data.append({
                    "Game": game_id,
                    "Market": market_title,
                    "Sub_Header": None,
                    "Player": current_player,
                    "Odds_1": line,
                    "Odds_2": None,
                    "Raw_Line": f"{current_player} {line}"
                })
                current_player = None 
        else:
            if len(line) > 2:
                current_player = line
                
import datetime
from datetime import timedelta

def extract_game_date(driver):
    """
    Attempts to extract the game date from the page.
    Returns date in YYYY-MM-DD format if found, else "Unknown".
    """
    today = datetime.date.today()
    
    try:
        # Strategy 1: "unavailable_after" meta tag (Best machine-readable source)
        # Format: Mon, 05 Jan 2026 08:00:00 UTC
        try:
            meta = driver.find_element(By.CSS_SELECTOR, "meta[name='robots']")
            content = meta.get_attribute("content")
            if "unavailable_after:" in content:
                date_part = content.split("unavailable_after:")[1].strip()
                # Parse "Mon, 05 Jan 2026 08:00:00 UTC"
                # We only care about the date. Since it's usually early morning UTC of the next day 
                # for evening games in NA, we might need to be careful.
                # However, usually the game date is the day *before* the expiry for NA night games.
                # Let's try to parse it.
                # Simplified: Just grab DD Jan YYYY
                match = re.search(r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})\s+(\d{2}):(\d{2}):(\d{2})", date_part)
                if match:
                    day, month_str, year, hour, minute, second = match.groups()
                    dt = datetime.datetime.strptime(f"{day} {month_str} {year} {hour}:{minute}:{second}", "%d %b %Y %H:%M:%S")
                    
                    # Heuristic: If hour is < 12 (08:00 UTC), it's likely the next day in UTC compared to game time.
                    # Subtract 12 hours to get safe "Game Day" in North America
                    game_dt = dt - timedelta(hours=12)
                    return game_dt.strftime("%Y-%m-%d")
        except NoSuchElementException:
            pass

        # Strategy 2: Look for "Today" or "Tomorrow" in start time text
        # Usually inside class "eventCardEventStartTimeText" or similar
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text[:10000] # Increased range
            if "Starts in" in body_text or "1h" in body_text or "2h" in body_text: # Very rough heuristic for Today
                 # This is risky if "Starts in" isn't unique.
                 pass
            
            # Search for specific text "Today" or "Tomorrow" if the site uses it (PlayNow might just use countdowns)
            # But let's check for standard date formats again in body
            date_regex = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-uary]*\.?\s+(\d{1,2}),?\s+(\d{4})"
            match = re.search(date_regex, body_text, re.IGNORECASE)
            if match:
                # Parse found date "Jan 4, 2026"
                month_str = match.group(1)
                day = match.group(2)
                year = match.group(3)
                dt = datetime.datetime.strptime(f"{day} {month_str} {year}", "%d %b %Y")
                return dt.strftime("%Y-%m-%d")

        except Exception:
            pass
            
        # Strategy 3: Yahoo Pixel (Last Resort, assumes scrape time ~= game time)
        # Only use if we are confident the game is today/soon.
        # But for backfilling or general scraping, this is bad. 
        # However, for "upcoming games", if no date is shown, it implies immediate future.
        
        # If we found nothing, and the page loaded successfully, maybe we can assume Today if there's a countdown?
        # Let's try to find countdown text "Xh Ym"
        countdown_regex = r"(\d{1,2})h\s+(\d{1,2})m"
        if re.search(countdown_regex, body_text):
             return today.strftime("%Y-%m-%d")

    except Exception as e:
        print(f"Warning: Could not extract game date: {e}")
    
    # Save page source if date not found
    try:
        with open("debug_game_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("Saved debug_game_page.html for inspection.")
    except Exception as e2:
        print(f"Failed to save debug page: {e2}")
        
    return "Unknown"

def process_game(driver, game_url):
    print(f"Processing {game_url}...")
    driver.get(game_url)
    
    # 1. Click "Player" tab
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='tab']"))
        )
        tabs = driver.find_elements(By.XPATH, "//div[@role='tab']")
        player_tab = None
        for tab in tabs:
            if "Player" in tab.text:
                player_tab = tab
                break
        if player_tab:
            if player_tab.get_attribute("aria-selected") != "true":
                print("Clicking 'Player' tab...")
                safe_click(driver, player_tab)
                time.sleep(2)
        else:
            print("Player tab not found.")
            return []
    except TimeoutException:
        print("Timeout waiting for tabs.")
        return []

    # 2. Expand All
    expand_all(driver)
    
    # 3. Scrape Data
    data = []
    game_id = game_url.split("/")[-1]
    
    game_date = extract_game_date(driver)
    print(f"Extracted Game Date: {game_date}")
    
    try:
        expand_icons = driver.find_elements(By.CSS_SELECTOR, "div[title='Expand or collapse']")
        seen_markets = set()
        
        for icon in expand_icons:
            try:
                header = icon.find_element(By.XPATH, "./..")
                market_title = header.text.split('\n')[0].strip()
                
                if market_title in seen_markets:
                    continue
                seen_markets.add(market_title)
                
                market_wrapper = header.find_element(By.XPATH, "./..")
                lines = market_wrapper.text.split('\n')
                
                # print(f"Parsing market: {market_title}")
                
                if market_title == "Goal Scorer":
                    market_data = parse_goal_scorer_market(lines, game_id, market_title)
                else:
                    market_data = parse_generic_market(lines, game_id, market_title)
                
                data.extend(market_data)

            except Exception as e:
                continue

    except Exception as e:
        print(f"Error scraping markets: {e}")
    
    for record in data:
        record['Game_Date'] = game_date
        
    return data

def main():
    main_url = "https://www.playnow.com/sports/sports/competition/220/hockey/north-america/nhl/matches"
    driver = setup_driver()
    all_data = []
    
    list_only = "--list-games" in sys.argv
    test_mode = "--test" in sys.argv

    try:
        # Increase wait time for game links
        game_links = get_game_links(driver, main_url)
        
        if not game_links:
            print("Scraping game links failed. Using hardcoded URLs for testing parsing logic.")
            game_links = [
                "https://www.playnow.com/sports/sports/event/11825486/hockey/north-america/nhl/pittsburgh-penguins-at-detroit-red-wings",
                "https://www.playnow.com/sports/sports/event/11825254/hockey/north-america/nhl/buffalo-sabres-at-columbus-blue-jackets",
                "https://www.playnow.com/sports/sports/event/11825352/hockey/north-america/nhl/utah-mammoth-at-new-jersey-devils"
            ]
        
        # Deduplicate links
        unique_links = []
        seen_urls = set()
        for link in game_links:
            # Normalize URL to remove query parameters for deduplication if needed, 
            # or just use the full string if they are distinct events.
            # Here we keep exact links but remove duplicates.
            if link not in seen_urls:
                unique_links.append(link)
                seen_urls.add(link)
        
        print(f"\nFound {len(unique_links)} unique games:")
        for i, link in enumerate(unique_links):
            print(f"{i+1}. {link}")

        if list_only:
            print("\nList mode enabled. Exiting without scraping props.")
            return

        if test_mode:
            print("\nTest mode enabled. Limiting to first game only.")
            unique_links = unique_links[:1]
        
        print(f"\nProcessing {len(unique_links)} unique games.")
        
        for i, link in enumerate(unique_links):
            print(f"[{i+1}/{len(unique_links)}] Processing game...")
            try:
                game_data = process_game(driver, link)
                all_data.extend(game_data)
            except Exception as e:
                print(f"Failed to process game {link}: {e}")
            
    except Exception as e:
        print(f"An error occurred in main execution: {e}")
    finally:
        driver.quit()
        
        # Save to CSV (Robust save)
        if all_data:
            df = pd.DataFrame(all_data)
            filenames = ["nhl_player_props.csv", "nhl_player_props_all.csv", "nhl_player_props_v2.csv", f"nhl_player_props_{int(time.time())}.csv"]
            
            for filename in filenames:
                try:
                    df.to_csv(filename, index=False)
                    print(f"Data successfully saved to {filename}")
                    print(f"Total records: {len(df)}")
                    break
                except PermissionError:
                    print(f"Permission denied for {filename}, trying next...")
                except Exception as e:
                    print(f"Error saving to {filename}: {e}")
        elif not list_only:
            print("No data collected to save.")

if __name__ == "__main__":
    main()
