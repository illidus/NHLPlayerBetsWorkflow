import duckdb
import argparse
import logging
from pathlib import Path
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def setup_db(con, force=False):
    """
    Sets up the initial tables (drops them if force=True).
    """
    if force:
        logger.info("Force flag set. Dropping existing tables...")
        con.execute("DROP TABLE IF EXISTS dim_players")
        con.execute("DROP TABLE IF EXISTS fact_skater_game_situation")
        con.execute("DROP TABLE IF EXISTS fact_skater_game_all")
        con.execute("DROP TABLE IF EXISTS fact_goalie_game_situation")
        con.execute("DROP TABLE IF EXISTS dim_games")
    
    pass

def ingest_players(con, data_root):
    """
    Ingests player lookup data.
    """
    root_path = Path(data_root)
    lookup_file = root_path.parent / "allPlayersLookup.csv"
    
    if not lookup_file.exists():
        logger.warning(f"Upstream returned 403; downloader skipped; ingestion may proceed using existing local data. Missing: {lookup_file}")
        return

    logger.info(f"Ingesting players from {lookup_file}...")
    
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'dim_players'").fetchone()[0] > 0
    
    if not table_exists:
        lookup_path = str(lookup_file).replace('\\', '/')
        query = f"""
        CREATE TABLE dim_players AS 
        SELECT 
            playerId as player_id,
            name as player_name,
            position,
            team,
            birthDate as birth_date,
            weight,
            height,
            nationality,
            shootsCatches as shoots_catches,
            primaryPosition as primary_position
        FROM read_csv_auto('{lookup_path}', union_by_name=True)
        """
        con.execute(query)
        logger.info("Created dim_players table.")
    else:
        logger.info("dim_players table already exists. Skipping.")

def ingest_skaters(con, data_root, start_season, end_season, season_type):
    """
    Ingests skater game-by-game data.
    """
    logger.info("Ingesting skater data...")
    
    sit_table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_skater_game_situation'").fetchone()[0] > 0
    
    if sit_table_exists:
        logger.info("fact_skater_game_situation already exists. Skipping raw ingest (use --force to rebuild).")
    else:
        files = []
        for season in range(start_season, end_season + 1):
            season_path = Path(data_root) / str(season) / season_type / "skaters"
            if season_path.exists():
                season_files = list(season_path.glob("*.csv"))
                files.extend([str(f).replace('\\', '/') for f in season_files])
            else:
                logger.warning(f"Season path not found: {season_path}")
        
        if not files:
            logger.warning("No skater files found.")
            return

        logger.info(f"Found {len(files)} skater files.")
        
        logger.info("Creating fact_skater_game_situation...")
        con.execute(f"""
        CREATE TABLE fact_skater_game_situation AS
        SELECT
            playerId as player_id,
            gameId as game_id,
            strptime(CAST(gameDate AS VARCHAR), '%Y%m%d') as game_date,
            CAST(gameId / 1000000 AS INTEGER) as season,
            playerTeam as team,
            opposingTeam as opp_team,
            home_or_away,
            position,
            situation,
            icetime as toi_seconds,
            I_F_goals as goals,
            I_F_primaryAssists as primary_assists,
            I_F_secondaryAssists as secondary_assists,
            I_F_points as points,
            I_F_shotsOnGoal as sog,
            shotsBlockedByPlayer as blocks,
            I_F_shotAttempts as shot_attempts,
            I_F_hits as hits,
            I_F_takeaways as takeaways,
            I_F_giveaways as giveaways,
            I_F_dZoneGiveaways as d_zone_giveaways,
            I_F_xGoals as x_goals,
            OnIce_F_xGoals as on_ice_xgoals,
            OnIce_F_goals as on_ice_goals
        FROM read_csv_auto({files}, union_by_name=True, filename=True)
        """
        )
        logger.info("Created fact_skater_game_situation.")

    all_table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_skater_game_all'").fetchone()[0] > 0
    
    if all_table_exists:
        logger.info("fact_skater_game_all already exists. Skipping.")
    else:
        logger.info("Creating fact_skater_game_all...")
        
        con.execute("""
        CREATE TABLE fact_skater_game_all AS
        WITH all_situations AS (
            SELECT * FROM fact_skater_game_situation WHERE situation = 'all'
        ),
        pp_toi AS (
            SELECT player_id, game_id, toi_seconds as pp_toi_seconds 
            FROM fact_skater_game_situation 
            WHERE situation = '5on4'
        ),
        ev_toi AS (
            SELECT player_id, game_id, toi_seconds as ev_toi_seconds 
            FROM fact_skater_game_situation 
            WHERE situation = '5on5'
        )
        SELECT
            a.player_id,
            a.game_id,
            a.game_date,
            a.season,
            a.team,
            a.opp_team,
            a.home_or_away,
            a.position,
            a.goals,
            COALESCE(a.primary_assists + a.secondary_assists, a.points - a.goals) as assists,
            a.points,
            a.sog,
            a.blocks,
            a.toi_seconds,
            a.toi_seconds / 60.0 as toi_minutes,
            COALESCE(p.pp_toi_seconds, 0) as pp_toi_seconds,
            COALESCE(e.ev_toi_seconds, 0) as ev_toi_seconds,
            a.x_goals,
            a.shot_attempts,
            a.hits
        FROM all_situations a
        LEFT JOIN pp_toi p ON a.player_id = p.player_id AND a.game_id = p.game_id
        LEFT JOIN ev_toi e ON a.player_id = e.player_id AND a.game_id = e.game_id
        """
        )
        logger.info("Created fact_skater_game_all.")

def ingest_goalies(con, data_root, start_season, end_season, season_type):
    """
    Ingests goalie game-by-game data.
    """
    logger.info("Ingesting goalie data...")
    
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'fact_goalie_game_situation'").fetchone()[0] > 0
    
    if table_exists:
        logger.info("fact_goalie_game_situation already exists. Skipping.")
        return

    files = []
    for season in range(start_season, end_season + 1):
        season_path = Path(data_root) / str(season) / season_type / "goalies"
        if season_path.exists():
            season_files = list(season_path.glob("*.csv"))
            files.extend([str(f).replace('\\', '/') for f in season_files])
    
    if not files:
        logger.warning("No goalie files found.")
        return

    logger.info(f"Found {len(files)} goalie files.")
    
    con.execute(f"""
    CREATE TABLE fact_goalie_game_situation AS
    SELECT
        playerId as player_id,
        gameId as game_id,
        strptime(CAST(gameDate AS VARCHAR), '%Y%m%d') as game_date,
        CAST(gameId / 1000000 AS INTEGER) as season,
        playerTeam as team,
        opposingTeam as opp_team,
        home_or_away,
        situation,
        icetime as toi_seconds,
        ongoal as shots_against,
        goals as goals_against,
        xGoals as x_goals_against
    FROM read_csv_auto({files}, union_by_name=True, filename=True)
    """
    )
    logger.info("Created fact_goalie_game_situation.")

def derive_games(con):
    """
    Derives dim_games from fact tables.
    """
    logger.info("Deriving dim_games...")
    
    table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'dim_games'").fetchone()[0] > 0
    
    if table_exists:
        logger.info("dim_games already exists. Skipping.")
        return

    con.execute("""
    CREATE TABLE dim_games AS
    SELECT
        game_id,
        MIN(game_date) as game_date,
        MIN(season) as season,
        MIN(CASE WHEN home_or_away = 'HOME' THEN team ELSE opp_team END) as home_team,
        MIN(CASE WHEN home_or_away = 'AWAY' THEN team ELSE opp_team END) as away_team
    FROM fact_skater_game_situation
    WHERE situation = 'all'
    GROUP BY game_id
    """
    )
    
    logger.info("Created dim_games.")

def main():
    parser = argparse.ArgumentParser(description="Ingest MoneyPuck data into DuckDB.")
    parser.add_argument("--start-season", type=int, default=2018)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--season-type", type=str, default="regular")
    parser.add_argument("--data-root", type=str, default=r"data\raw\moneypuck\teamPlayerGameByGame")
    parser.add_argument("--duckdb-path", type=str, default=r"data\db\nhl_backtest.duckdb")
    parser.add_argument("--force", action="store_true", help="Drop existing tables and rebuild")
    
    args = parser.parse_args()
    
    logger.info(f"Starting ingestion with args: {args}")
    
    Path(args.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(args.duckdb_path)
    
    try:
        setup_db(con, args.force)
        ingest_players(con, args.data_root)
        ingest_skaters(con, args.data_root, args.start_season, args.end_season, args.season_type)
        ingest_goalies(con, args.data_root, args.start_season, args.end_season, args.season_type)
        derive_games(con)
        
        logger.info("Ingestion complete.")
        
        tables = con.execute("SHOW TABLES").fetchall()
        for t in tables:
            count = con.execute(f"SELECT count(*) FROM {t[0]}").fetchone()[0]
            logger.info(f"Table {t[0]}: {count} rows")
            
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()
