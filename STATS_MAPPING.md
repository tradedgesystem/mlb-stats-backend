I ade# Stats Mapping

This maps the requested stats to the current SQLite columns. If a stat is
marked "needs new data", it is not in the current `batting_stats` or
`pitching_stats` tables and requires a new ingest source (often Statcast
batted-ball or pitch-level data).

## Hitters

### Outcome & Slash (available now)
- Plate Appearances (PA) -> `pa`
- At-Bats (AB) -> `ab`
- Hits (H) -> `h`
- Singles (1B) -> `1b`
- Doubles (2B) -> `2b`
- Triples (3B) -> `3b`
- Home Runs (HR) -> `hr`
- Runs (R) -> `r`
- Runs Batted In (RBI) -> `rbi`
- Walks (BB) -> `bb`
- Intentional Walks (IBB) -> `ibb`
- Hit By Pitch (HBP) -> `hbp`
- Strikeouts (SO) -> `so`
- Sacrifice Flies (SF) -> `sf`
- Sacrifice Hits (SH) -> `sh`
- Batting Average (AVG) -> `avg`
- On-Base Percentage (OBP) -> `obp`
- Slugging Percentage (SLG) -> `slg`
- On-Base Plus Slugging (OPS) -> `ops`
- Isolated Power (ISO) -> `iso`
- Batting Average on Balls in Play (BABIP) -> `babip`

### Expected / Statcast Quality
- Expected Batting Average (xBA) -> `xba` (available)
- Expected Slugging (xSLG) -> `xslg` (available)
- Expected On-Base Percentage (xOBP) -> `xobp` (needs new data)
- Expected Weighted On-Base Average (xwOBA) -> `xwoba` (available)
- Weighted On-Base Average (wOBA) -> `woba` (available)
- Expected Home Runs (xHR) -> `xhr` (needs new data)

### Contact & Power
- Average Exit Velocity -> `ev` (available)
- Max Exit Velocity -> `maxev` (available)
- Median Exit Velocity -> `median_ev` (available via statcast range)
- Exit Velocity Percentiles (10th, 50th, 90th) -> `ev_p10`, `ev_p50`, `ev_p90` (available via statcast range)
- Hard-Hit % -> `hardhitpct` (available)
- Barrel Count -> `barrels` (available)
- Barrel % -> `barrelpct` (available)
- Barrels per Plate Appearance -> `barrels_per_pa` (available)
- Barrels per Ball in Play -> `barrels_per_bip` (available via statcast range)
- Sweet-Spot % -> `sweet_spot_pct` (available via statcast range)
- Average Launch Angle -> `la` (available)
- Launch Angle Standard Deviation -> `la_sd` (available via statcast range)

### Batted-Ball Type
- Ground Ball % -> `gbpct` (available)
- Line Drive % -> `ldpct` (available)
- Fly Ball % -> `fbpct` (available)
- Pop-Up % -> `popct` (available)
- Ground Balls per Plate Appearance -> `gb_per_pa` (available)
- Fly Balls per Plate Appearance -> `fb_per_pa` (available)
- Line Drives per Plate Appearance -> `ld_per_pa` (available)
- Infield Fly % -> `iffbpct` (available)

### Direction & Spray
- Pull % -> `pullpct` (available)
- Center % -> `centpct` (available)
- Oppo % -> `oppopct` (available)
- Pull Air % -> `pull_air_pct` (needs new data)
- Oppo Air % -> `oppo_air_pct` (needs new data)
- Pulled Ground Ball % -> `pull_gb_pct` (needs new data)
- Oppo Ground Ball % -> `oppo_gb_pct` (needs new data)
- Straightaway % -> `straightaway_pct` (needs new data)
- Shifted Plate Appearance % -> `shifted_pa_pct` (needs new data)
- Non-Shifted Plate Appearance % -> `non_shifted_pa_pct` (needs new data)

### Plate Discipline
- Swing % -> `swingpct` (available)
- Swing Outside Zone % (O-Swing%) -> `o_swingpct` (available)
- Swing Inside Zone % (Z-Swing%) -> `z_swingpct` (available)
- Contact % -> `contactpct` (available)
- Contact Outside Zone % (O-Contact%) -> `o_contactpct` (available)
- Contact Inside Zone % (Z-Contact%) -> `z_contactpct` (available)
- Whiff % -> `whiffpct` (available via statcast range)
- Called Strike % -> `cstrpct` (available)
- Swinging Strike % -> `swstrpct` (available)
- Foul % -> `foulpct` (available via statcast range)
- Foul Tip % -> `foul_tip_pct` (available via statcast range)
- In-Play % -> `in_play_pct` (available via statcast range)
- Take % -> `take_pct` (available via statcast range)
- Take in Zone % -> `take_in_zone_pct` (available via statcast range)
- Take out of Zone % -> `take_out_zone_pct` (available via statcast range)
- First-Pitch Swing % -> `first_pitch_swing_pct` (available via statcast range)
- First-Pitch Take % -> `first_pitch_take_pct` (available via statcast range)
- Two-Strike Swing % -> `two_strike_swing_pct` (available via statcast range)
- Two-Strike Whiff % -> `two_strike_whiff_pct` (available via statcast range)

### Contact Quality Buckets
- Under % -> `under_pct` (available via statcast range)
- Topped % -> `ttopct` (available)
- Flare/Burner % -> `flare_burner_pct` (available via statcast range)
- Solid Contact % -> `hardpct` (available)
- Weak Contact % -> `softpct` (available)
- Poorly Hit % -> `poorly_hit_pct` (available via statcast range)
- Poorly Under % -> `poorly_under_pct` (available via statcast range)
- Poorly Topped % -> `poorly_topped_pct` (available via statcast range)
- Poorly Weak % -> `poorly_weak_pct` (available via statcast range)

### Count & Context
- Ahead-in-Count % -> `ahead_in_count_pct` (needs new data)
- Even-Count % -> `even_count_pct` (needs new data)
- Behind-in-Count % -> `behind_in_count_pct` (needs new data)
- Two-Strike Plate Appearance % -> `two_strike_pa_pct` (needs new data)
- Three-Ball Plate Appearance % -> `three_ball_pa_pct` (needs new data)
- Late and Close Plate Appearances -> `late_close_pa` (needs new data)
- Leverage Index -> `pli` (available)

## Pitchers

### Outcomes & Rates (available now)
- Games (G) -> `g`
- Games Started (GS) -> `gs`
- Innings Pitched (IP) -> `ip`
- Batters Faced (BF) -> `tbf`
- Hits Allowed (H) -> `h`
- Runs Allowed (R) -> `r`
- Earned Runs (ER) -> `er`
- Home Runs Allowed (HR) -> `hr`
- Walks Allowed (BB) -> `bb`
- Hit Batters (HBP) -> `hbp`
- Strikeouts (SO) -> `so`
- Earned Run Average (ERA) -> `era`
- Walks plus Hits per Inning Pitched (WHIP) -> `whip`
- Strikeouts per Nine (K/9) -> `k_9`
- Walks per Nine (BB/9) -> `bb_9`
- Home Runs per Nine (HR/9) -> `hr_9`
- Strikeout Minus Walk Rate (K-BB%) -> `k_bbpct`

### Expected & Contact Allowed
- Expected ERA (xERA) -> `xera` (available)
- Expected Weighted On-Base Average Allowed (xwOBA) -> `xwoba_allowed` (needs new data)
- Weighted On-Base Average Allowed (wOBA) -> `woba_allowed` (needs new data)
- Batting Average Allowed (BAA) -> `avg` (available)
- Slugging Allowed (SLG) -> `slg_allowed` (needs new data)
- Average Exit Velocity Allowed -> `ev` (available)
- Max Exit Velocity Allowed -> `maxev` (available)
- Exit Velocity Percentiles Allowed (10th, 50th, 90th) -> `ev_p10_allowed`, `ev_p50_allowed`, `ev_p90_allowed` (needs new data)
- Barrel % Allowed -> `barrelpct` (available)
- Sweet-Spot % Allowed -> `sweet_spot_pct_allowed` (needs new data)

### Pitch Arsenal
- Pitch Usage % (by pitch type) -> `fbpct_2`, `slpct`, `ctpct`, `cbpct`, `chpct`, `sfpct`, `knpct`, `xxpct` (available)
- Average Velocity (by pitch type) -> `fbv`, `slv`, `ctv`, `cbv`, `chv`, `sfv`, `knv` (available)
- Max Velocity -> `max_velo` (needs new data)
- Velocity Standard Deviation -> `velo_sd` (needs new data)
- Spin Rate -> `spin_rate` (needs new data)
- Spin Rate Standard Deviation -> `spin_sd` (needs new data)
- Spin Axis -> `spin_axis` (needs new data)
- Extension -> `extension` (needs new data)
- Release Height -> `release_height` (needs new data)
- Release Side -> `release_side` (needs new data)

### Pitch Results
- Whiff % -> `whiffpct` (needs new data)
- Chase % -> `o_swingpct` (available)
- Called Strikes plus Whiffs (CSW%) -> `cswpct` (available)
- Strike % -> `strike_pct` (needs new data)
- Called Strike % -> `cstrpct` (available)
- Swinging Strike % -> `swstrpct` (available)
- Ground Ball % -> `gbpct` (available)
- Fly Ball % -> `fbpct` (available)
- Line Drive % -> `ldpct` (available)
- Pop-Ups Forced -> `iffbpct` (available)

### Usage & Sequencing
- Primary Pitch % -> `primary_pitch_pct` (needs new data)
- Secondary Pitch % -> `secondary_pitch_pct` (needs new data)
