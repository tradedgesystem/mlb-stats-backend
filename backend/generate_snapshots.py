#!/usr/bin/env python3
"""
Generate snapshot JSON files from the stats database for the Chrome extension.
These snapshots include all columns including fWAR (fwarc), wRC+, and FIP.
"""
import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

# Database path
DB_PATH = "stats.db"

# Output directory for snapshots
SNAPSHOTS_DIR = Path("../extension/snapshots")

# Seasons to generate snapshots for
SEASONS = [2024, 2025]


def get_connection():
    """Get database connection."""
    return sqlite3.connect(DB_PATH)


def query_batting_stats(conn, season):
    """Query batting stats for a specific season."""
    query = """
    SELECT player_id, name, team, season, age, g, ab, pa, h, 
           `1b`, `2b`, `3b`, hr, r, rbi, bb, ibb, so, hbp, 
           sf, sh, gdp, sb, cs, avg, gb, fb, ld, iffb, 
           pitches, balls, strikes, ifh, bu, buh, bbpct, kpct, 
           bb_k, obp, slg, ops, iso, babip, gb_fb, ldpct, 
           gbpct, fbpct, iffbpct, hr_fb, ifhpct, buhpct, 
           woba, wraa, wrc, bat, fld, rep, pos, rar, fwarc, 
           dol, spd, wrc_plus, wpa, wpa_2, pluswpa, re24, 
           rew, pli, phli, ph, wpa_li, clutch, fbpct_pitch, 
           fbv, slpct, slv, ctpct, ctv, cbpct, cbv, chpct, 
           chv, sfpct, sfv, knpct, knv, xxpct, popct, wfb, 
           wsl, wct, wcb, wch, wsf, wkn, wfb_c, wsl_c, 
           wct_c, wcb_c, wch_c, wsf_c, wkn_c, o_swingpct, 
           z_swingpct, swingpct, o_contactpct, z_contactpct, 
           contactpct, zonepct, f_strikepct, swstrpct, bsr, 
           fapct_sc, ftpct_sc, fcpct_sc, fspct_sc, fopct_sc, 
           sipct_sc, slpct_sc, cupct_sc, kcpct_sc, eppct_sc, 
           chpct_sc, scpct_sc, knpct_sc, unpct_sc, vfa_sc, 
           vft_sc, vfc_sc, vfs_sc, vfo_sc, vsi_sc, vsl_sc, 
           vcu_sc, vkc_sc, vep_sc, vch_sc, vsc_sc, vkn_sc, 
           fa_x_sc, ft_x_sc, fc_x_sc, fs_x_sc, fo_x_sc, 
           si_x_sc, sl_x_sc, cu_x_sc, kc_x_sc, ep_x_sc, 
           ch_x_sc, sc_x_sc, kn_x_sc, fa_z_sc, ft_z_sc, 
           fc_z_sc, fs_z_sc, fo_z_sc, si_z_sc, sl_z_sc, 
           cu_z_sc, kc_z_sc, ep_z_sc, ch_z_sc, sc_z_sc, 
           kn_z_sc, wfa_sc, wft_sc, wfc_sc, wfs_sc, wfo_sc, 
           wsi_sc, wsl_sc, wcu_sc, wkc_sc, wep_sc, wch_sc, 
           wsc_sc, wkn_sc, wfa_c_sc, wft_c_sc, wfc_c_sc, 
           wfs_c_sc, wfo_c_sc, wsi_c_sc, wsl_c_sc, wcu_c_sc, 
           wkc_c_sc, wep_c_sc, wch_c_sc, wsc_c_sc, wkn_c_sc, 
           o_swingpct_sc, z_swingpct_sc, swingpct_sc, 
           o_contactpct_sc, z_contactpct_sc, contactpct_sc, 
           zonepct_sc, pace, def, wsb, ubr, age_rng, off, 
           lg, wgdp, pullpct, centpct, oppopct, softpct, 
           medpct, hardpct, ttopct, chpct_pi, cspct_pi, 
           cupct_pi, fapct_pi, fcpct_pi, fspct_pi, knpct_pi, 
           sbpct_pi, sipct_pi, slpct_pi, xxpct_pi, vch_pi, 
           vcs_pi, vcu_pi, vfa_pi, vfc_pi, vfs_pi, vkn_pi, 
           vsb_pi, vsi_pi, vsl_pi, vxx_pi, ch_x_pi, 
           cs_x_pi, cu_x_pi, fa_x_pi, fc_x_pi, fs_x_pi, 
           kn_x_pi, sb_x_pi, si_x_pi, sl_x_pi, xx_x_pi, 
           ch_z_pi, cs_z_pi, cu_z_pi, fa_z_pi, fc_z_pi, 
           fs_z_pi, kn_z_pi, sb_z_pi, si_z_pi, sl_z_pi, 
           xx_z_pi, wch_pi, wcs_pi, wcu_pi, wfa_pi, wfc_pi, 
           wfs_pi, wkn_pi, wsb_pi, wsi_pi, wsl_pi, wxx_pi, 
           wch_c_pi, wcs_c_pi, wcu_c_pi, wfa_c_pi, wfc_c_pi, 
           wfs_c_pi, wkn_c_pi, wsb_c_pi, wsi_c_pi, 
           wsl_c_pi, wxx_c_pi, o_swingpct_pi, z_swingpct_pi, 
           swingpct_pi, o_contactpct_pi, z_contactpct_pi, 
           contactpct_pi, zonepct_pi, pace_pi, frm, avg_plus, 
           bbpct_plus, kpct_plus, obp_plus, slg_plus, iso_plus, 
           babip_plus, ld_pluspct, gbpct_plus, fbpct_plus, 
           hr_fbpct_plus, pullpct_plus, centpct_plus, 
           oppopct_plus, softpct_plus, medpct_plus, 
           hardpct_plus, ev, la, barrels, barrelpct, maxev, 
           hardhit, hardhitpct, events, cstrpct, cswpct, xba, 
           xslg, xwoba, l_war, barrels_per_pa, gb_per_pa, 
           fb_per_pa, ld_per_pa
    FROM batting_stats 
    WHERE season = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, (season,))
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    
    players = []
    for row in rows:
        player = dict(zip(columns, row))
        # Convert sqlite3 objects to Python types
        for key, value in player.items():
            if isinstance(value, bytes):
                player[key] = value.decode('utf-8')
            elif value is None:
                player[key] = None
        players.append(player)
    
    return players


def query_pitching_stats(conn, season):
    """Query pitching stats for a specific season."""
    # First check if pitching_stats table exists and has data
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='pitching_stats'
    """)
    if not cursor.fetchone():
        return []
    
    # Use SELECT * to get all columns from the table
    query = "SELECT * FROM pitching_stats WHERE season = ?"
    cursor.execute(query, (season,))
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()
    
    players = []
    for row in rows:
        player = dict(zip(columns, row))
        # Convert sqlite3 objects to Python types
        for key, value in player.items():
            if isinstance(value, bytes):
                player[key] = value.decode('utf-8')
            elif value is None:
                player[key] = None
        players.append(player)
    
    return players


def generate_snapshot(players, season, data_type="players"):
    """Generate snapshot JSON with metadata."""
    now = datetime.utcnow().isoformat()
    
    # Check if we're in 2025 and use different source label
    if season == 2025:
        source = "bundled snapshot"
    else:
        source = "cdn snapshot"
    
    snapshot = {
        "players": players,
        "meta": {
            "generated_at": now,
            "source": source,
            "season": season,
            "data_type": data_type
        }
    }
    
    return snapshot


def save_snapshot(snapshot, season, data_type="players"):
    """Save snapshot to JSON file."""
    filename = f"{data_type}_{season}.json"
    filepath = SNAPSHOTS_DIR / filename
    
    # Ensure directory exists
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"✓ Generated {filename} ({len(snapshot['players'])} players)")
    return filepath


def main():
    """Main function to generate all snapshots."""
    print("Generating snapshot files from database...")
    print(f"Database: {DB_PATH}")
    print(f"Output directory: {SNAPSHOTS_DIR}")
    print(f"Seasons: {SEASONS}")
    print()
    
    conn = get_connection()
    
    try:
        # Generate batting snapshots
        for season in SEASONS:
            print(f"Processing batting stats for {season}...")
            players = query_batting_stats(conn, season)
            if players:
                snapshot = generate_snapshot(players, season, "players")
                save_snapshot(snapshot, season, "players")
            else:
                print(f"  No batting data found for {season}")
        
        print()
        
        # Generate pitching snapshots
        for season in SEASONS:
            print(f"Processing pitching stats for {season}...")
            players = query_pitching_stats(conn, season)
            if players:
                snapshot = generate_snapshot(players, season, "pitchers")
                save_snapshot(snapshot, season, "pitchers")
            else:
                print(f"  No pitching data found for {season}")
        
        print()
        print("=" * 50)
        print("Snapshot generation complete!")
        print("=" * 50)
        print()
        print(f"Files created in: {SNAPSHOTS_DIR.absolute()}")
        print()
        print("To update CDN:")
        print("1. Review the generated files")
        print("2. git add extension/snapshots/*.json")
        print("3. git commit -m 'Update snapshots with fWAR, wRC+, and FIP'")
        print("4. git push origin main")
        print()
        print("CDN will automatically update after push.")
        
    except Exception as e:
        print(f"Error generating snapshots: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
