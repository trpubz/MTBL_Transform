# MTBL_Transform
Transform function of MTBL ETL pipeline. The script needs to ingest x6 .json files and combine data. Compute wRAA and VORP for relative comparisons, among other activities.

## Dependencies
This script serves the transform function of the ETL Pipeline that depends on cleaned data from the Extract process.
Inputs for this process include:
- lg_managers.json
  - League Managers: sourced from ESPN; has the league managers' information
- lg_rosters.json
  - League Rosters: sourced from ESPN; has the rosters in a json dictionary.  Players stored as ESPNID strings.
- espn_player_universe.json
  - ESPN Player Universe: has OVR rank, eligible positions, and category Player Rater data. 
- bats_fg.csv
  - Hitter RoS Projections: sourced from Fangraphs, combined from multiple RoS projections 
- bats_stats.csv
  - Hitter Season Stats: sourced from Frangraphs, standard/advanced stats. 
- arms_fg.csv
  - Pitcher RoS Projections: sourced from Fangraphs, combined from multiple RoS projections 
- arms_stats.csv
  - Pitcher Season Stats: sourced from Frangraphs (QS data sourced from Baseball-Reference, standard/advanced stats. 
- arms_savant.csv
  - Pitcher Arsenal Stats: sourced from Savant 'Pitcher Arsenal Leaderboard'.  Broken down by pitch type with Wiff% info.   

## Outputs
This will output 2x .json files for Hitters/Pitchers.  It will have all current/projection data combined, with VORP computed
- bats_mtbl.json
- arms_mtbl.json