[![codecov](https://codecov.io/gh/trpubz/MTBL_Transform/graph/badge.svg?token=6d3GfveuUY)](https://codecov.io/gh/trpubz/MTBL_Transform)
[![covsunburst](https://codecov.io/gh/trpubz/MTBL_Transform/graph/sunburst.svg?token=6d3GfveuUY)](https://codecov.io/gh/trpubz/MTBL_Transform)
[![CircleCI](https://dl.circleci.com/status-badge/img/circleci/ND2c9oPVuFtQWAcK7DzGxc/D8zU1KuKBYZ7oyJZaUW7rX/tree/feature%2Fmtbl-69-add-keymap-encapsulation.svg?style=svg&circle-token=f2dcb42b614271e364161786d83493657f1569d8)](https://dl.circleci.com/status-badge/redirect/circleci/ND2c9oPVuFtQWAcK7DzGxc/D8zU1KuKBYZ7oyJZaUW7rX/tree/feature%2Fmtbl-69-add-keymap-encapsulation)

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
  - Pitcher Season Stats: sourced from Frangraphs (QS data sourced from Baseball-Reference, standard/advanced stats). 
- arms_savant.csv
  - Pitcher Arsenal Stats: sourced from Savant 'Pitcher Arsenal Leaderboard'.  Broken down by pitch type with Wiff% info.   

## Outputs
This will output 2x .json files for Hitters/Pitchers.  It will have all current/projection data combined, with VORP computed
- bats_mtbl.json
- arms_mtbl.json

## Performance Findings

Converting the raw data into a pandas Dataframe object and indexing on `playerid` yeilded ~ 260x performance increase.

<img width="2222" alt="Screenshot 2024-03-06 at 13 38 30" src="https://github.com/trpubz/MTBL_Transform/assets/25095319/1a94003f-89d4-44fd-b422-2366df6d82d8">
