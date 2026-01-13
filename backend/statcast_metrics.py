from __future__ import annotations

import numpy as np
import pandas as pd

STATCAST_BATTER_COLUMNS = [
    "swingpct",
    "o_swingpct",
    "z_swingpct",
    "contactpct",
    "o_contactpct",
    "z_contactpct",
    "whiffpct",
    "swstrpct",
    "cstrpct",
    "foulpct",
    "foul_tip_pct",
    "in_play_pct",
    "take_pct",
    "take_in_zone_pct",
    "take_out_zone_pct",
    "first_pitch_swing_pct",
    "first_pitch_take_pct",
    "two_strike_swing_pct",
    "two_strike_whiff_pct",
    "ev",
    "maxev",
    "median_ev",
    "ev_p10",
    "ev_p50",
    "ev_p90",
    "hardhitpct",
    "barrels",
    "barrelpct",
    "barrels_per_bip",
    "sweet_spot_pct",
    "la",
    "la_sd",
    "gbpct",
    "ldpct",
    "fbpct",
    "iffbpct",
    "gb_per_pa",
    "fb_per_pa",
    "ld_per_pa",
    "under_pct",
    "flare_burner_pct",
    "poorly_hit_pct",
    "poorly_under_pct",
    "poorly_topped_pct",
    "poorly_weak_pct",
    "pullpct",
    "centpct",
    "oppopct",
    "pull_air_pct",
    "oppo_air_pct",
    "straightaway_pct",
    "shifted_pa_pct",
    "non_shifted_pa_pct",
    "ahead_in_count_pct",
    "even_count_pct",
    "behind_in_count_pct",
    "two_strike_pa_pct",
    "three_ball_pa_pct",
    "late_close_pa",
    "pli",
    "xba",
    "xslg",
    "xobp",
    "xwoba",
]

SWING_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
    "foul_bunt",
    "bunt_foul_tip",
    "missed_bunt",
}

WHIFF_DESCRIPTIONS = {
    "swinging_strike",
    "swinging_strike_blocked",
    "missed_bunt",
}

FOUL_DESCRIPTIONS = {
    "foul",
    "foul_bunt",
}

FOUL_TIP_DESCRIPTIONS = {
    "foul_tip",
    "bunt_foul_tip",
}

IN_PLAY_DESCRIPTIONS = {
    "hit_into_play",
    "hit_into_play_no_out",
    "hit_into_play_score",
}

CALLED_STRIKE_DESCRIPTIONS = {
    "called_strike",
}

WALK_EVENTS = {"walk", "intent_walk"}
HBP_EVENTS = {"hit_by_pitch"}
SAC_FLY_EVENTS = {"sac_fly", "sac_fly_double_play"}
SAC_BUNT_EVENTS = {"sac_bunt", "sac_bunt_double_play"}
NON_AB_EVENTS = WALK_EVENTS | HBP_EVENTS | SAC_FLY_EVENTS | SAC_BUNT_EVENTS | {
    "catcher_interference"
}

STATCAST_REQUIRED_COLUMNS = {
    "player_id",
    "game_pk",
    "at_bat_number",
    "description",
    "zone",
    "pitch_number",
    "balls",
    "strikes",
    "bb_type",
    "launch_speed",
    "launch_angle",
    "launch_speed_angle",
    "hc_x",
    "hc_y",
    "stand",
    "type",
    "events",
    "if_fielding_alignment",
    "of_fielding_alignment",
    "inning",
    "bat_score",
    "fld_score",
    "delta_home_win_exp",
    "estimated_ba_using_speedangle",
    "estimated_slg_using_speedangle",
    "estimated_woba_using_speedangle",
    "woba_value",
    "woba_denom",
}

STATCAST_PITCHER_COLUMNS = [
    "fbpct_2",
    "slpct",
    "ctpct",
    "cbpct",
    "chpct",
    "sfpct",
    "knpct",
    "xxpct",
    "fbv",
    "slv",
    "ctv",
    "cbv",
    "chv",
    "sfv",
    "knv",
    "avg_velo",
    "max_velo",
    "velo_sd",
    "spin_rate",
    "spin_sd",
    "spin_axis",
    "extension",
    "release_height",
    "release_side",
]

STATCAST_PITCHER_REQUIRED_COLUMNS = {
    "pitcher",
    "pitch_type",
    "release_speed",
    "release_spin_rate",
    "spin_axis",
    "release_extension",
    "release_pos_x",
    "release_pos_z",
}

FASTBALL_TYPES = {"FF", "FT", "SI", "FA"}
SLIDER_TYPES = {"SL"}
CUTTER_TYPES = {"FC"}
CURVEBALL_TYPES = {"CU", "KC", "CS"}
CHANGEUP_TYPES = {"CH"}
SPLITTER_TYPES = {"FS", "FO", "SC"}
KNUCKLE_TYPES = {"KN"}


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.replace(0, pd.NA))


def build_statcast_batter_metrics_from_df(
    statcast_df: pd.DataFrame,
) -> pd.DataFrame:
    if "player_id" not in statcast_df.columns:
        return pd.DataFrame(columns=["player_id"] + STATCAST_BATTER_COLUMNS)

    statcast_df = statcast_df.copy()
    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["player_id"], errors="coerce"
    )
    statcast_df = statcast_df[statcast_df["player_id"].notna()].copy()
    if statcast_df.empty:
        return pd.DataFrame(columns=["player_id"] + STATCAST_BATTER_COLUMNS)

    statcast_df["player_id"] = statcast_df["player_id"].astype(int)

    present_cols = [
        col for col in STATCAST_REQUIRED_COLUMNS if col in statcast_df.columns
    ]
    statcast_df = statcast_df[present_cols]

    player_index = pd.Index(
        statcast_df["player_id"].dropna().unique(), name="player_id"
    )
    metrics = pd.DataFrame(index=player_index)
    pa_counts = None
    pa_last = None

    if "description" in statcast_df.columns:
        desc = statcast_df["description"].fillna("")
        swing_mask = desc.isin(SWING_DESCRIPTIONS)
        whiff_mask = desc.isin(WHIFF_DESCRIPTIONS)
        foul_mask = desc.isin(FOUL_DESCRIPTIONS)
        foul_tip_mask = desc.isin(FOUL_TIP_DESCRIPTIONS)
        in_play_mask = desc.isin(IN_PLAY_DESCRIPTIONS)
        called_strike_mask = desc.isin(CALLED_STRIKE_DESCRIPTIONS)
        contact_mask = swing_mask & ~whiff_mask

        total_pitches = statcast_df.groupby("player_id").size()
        swings = statcast_df.loc[swing_mask].groupby("player_id").size()
        whiffs = statcast_df.loc[whiff_mask].groupby("player_id").size()
        fouls = statcast_df.loc[foul_mask].groupby("player_id").size()
        foul_tips = statcast_df.loc[foul_tip_mask].groupby("player_id").size()
        in_play = statcast_df.loc[in_play_mask].groupby("player_id").size()
        takes = statcast_df.loc[~swing_mask].groupby("player_id").size()
        contacts = statcast_df.loc[contact_mask].groupby("player_id").size()
        called_strikes = statcast_df.loc[called_strike_mask].groupby("player_id").size()

        metrics["swingpct"] = safe_divide(swings, total_pitches)
        metrics["contactpct"] = safe_divide(contacts, swings)
        metrics["whiffpct"] = safe_divide(whiffs, swings)
        metrics["swstrpct"] = safe_divide(whiffs, total_pitches)
        metrics["cstrpct"] = safe_divide(called_strikes, total_pitches)
        metrics["foulpct"] = safe_divide(fouls, total_pitches)
        metrics["foul_tip_pct"] = safe_divide(foul_tips, total_pitches)
        metrics["in_play_pct"] = safe_divide(in_play, total_pitches)
        metrics["take_pct"] = safe_divide(takes, total_pitches)

        if "zone" in statcast_df.columns:
            zone = pd.to_numeric(statcast_df["zone"], errors="coerce")
            in_zone_mask = zone.between(1, 9)
            pitches_in_zone = statcast_df.loc[in_zone_mask].groupby("player_id").size()
            pitches_out_zone = statcast_df.loc[~in_zone_mask].groupby("player_id").size()
            takes_in_zone = statcast_df.loc[in_zone_mask & ~swing_mask].groupby(
                "player_id"
            ).size()
            takes_out_zone = statcast_df.loc[~in_zone_mask & ~swing_mask].groupby(
                "player_id"
            ).size()
            swings_in_zone = statcast_df.loc[in_zone_mask & swing_mask].groupby(
                "player_id"
            ).size()
            swings_out_zone = statcast_df.loc[~in_zone_mask & swing_mask].groupby(
                "player_id"
            ).size()
            contacts_in_zone = statcast_df.loc[
                in_zone_mask & contact_mask
            ].groupby("player_id").size()
            contacts_out_zone = statcast_df.loc[
                ~in_zone_mask & contact_mask
            ].groupby("player_id").size()

            metrics["take_in_zone_pct"] = safe_divide(
                takes_in_zone, pitches_in_zone
            )
            metrics["take_out_zone_pct"] = safe_divide(
                takes_out_zone, pitches_out_zone
            )
            metrics["z_swingpct"] = safe_divide(swings_in_zone, pitches_in_zone)
            metrics["o_swingpct"] = safe_divide(swings_out_zone, pitches_out_zone)
            metrics["z_contactpct"] = safe_divide(contacts_in_zone, swings_in_zone)
            metrics["o_contactpct"] = safe_divide(contacts_out_zone, swings_out_zone)

        if "pitch_number" in statcast_df.columns:
            pitch_number = pd.to_numeric(statcast_df["pitch_number"], errors="coerce")
            first_pitch_mask = pitch_number == 1
            first_pitches = statcast_df.loc[first_pitch_mask].groupby("player_id").size()
            first_swings = statcast_df.loc[
                first_pitch_mask & swing_mask
            ].groupby("player_id").size()
            first_takes = statcast_df.loc[
                first_pitch_mask & ~swing_mask
            ].groupby("player_id").size()

            metrics["first_pitch_swing_pct"] = safe_divide(
                first_swings, first_pitches
            )
            metrics["first_pitch_take_pct"] = safe_divide(
                first_takes, first_pitches
            )

        if "strikes" in statcast_df.columns:
            strikes = pd.to_numeric(statcast_df["strikes"], errors="coerce")
            two_strike_mask = strikes == 2
            two_strike_pitches = statcast_df.loc[two_strike_mask].groupby(
                "player_id"
            ).size()
            two_strike_swings = statcast_df.loc[
                two_strike_mask & swing_mask
            ].groupby("player_id").size()
            two_strike_whiffs = statcast_df.loc[
                two_strike_mask & whiff_mask
            ].groupby("player_id").size()

            metrics["two_strike_swing_pct"] = safe_divide(
                two_strike_swings, two_strike_pitches
            )
            metrics["two_strike_whiff_pct"] = safe_divide(
                two_strike_whiffs, two_strike_swings
            )

    if {
        "game_pk",
        "at_bat_number",
        "pitch_number",
        "balls",
        "strikes",
    }.issubset(statcast_df.columns):
        pa_cols = statcast_df[
            [
                "player_id",
                "game_pk",
                "at_bat_number",
                "pitch_number",
                "balls",
                "strikes",
                "inning",
                "bat_score",
                "fld_score",
                "delta_home_win_exp",
                "if_fielding_alignment",
                "of_fielding_alignment",
            ]
        ].copy()
        pa_cols["game_pk"] = pd.to_numeric(pa_cols["game_pk"], errors="coerce")
        pa_cols["at_bat_number"] = pd.to_numeric(
            pa_cols["at_bat_number"], errors="coerce"
        )
        pa_cols["pitch_number"] = pd.to_numeric(
            pa_cols["pitch_number"], errors="coerce"
        )
        pa_cols["balls"] = pd.to_numeric(pa_cols["balls"], errors="coerce")
        pa_cols["strikes"] = pd.to_numeric(pa_cols["strikes"], errors="coerce")
        pa_cols = pa_cols.dropna(subset=["game_pk", "at_bat_number", "pitch_number"])

        if not pa_cols.empty:
            group_cols = ["player_id", "game_pk", "at_bat_number"]
            pa_group = pa_cols.groupby(group_cols, sort=False)
            last_idx = pa_group["pitch_number"].idxmax()
            pa_last = pa_cols.loc[last_idx].copy()
            pa_counts = pa_last.groupby("player_id").size().reindex(
                player_index, fill_value=0
            )

            max_balls = pa_group["balls"].max()
            max_strikes = pa_group["strikes"].max()

            ahead = (pa_last["balls"] > pa_last["strikes"]).groupby(
                pa_last["player_id"]
            ).sum()
            even = (pa_last["balls"] == pa_last["strikes"]).groupby(
                pa_last["player_id"]
            ).sum()
            behind = (pa_last["balls"] < pa_last["strikes"]).groupby(
                pa_last["player_id"]
            ).sum()
            two_strike_pa = (max_strikes >= 2).groupby("player_id").sum()
            three_ball_pa = (max_balls >= 3).groupby("player_id").sum()

            metrics["ahead_in_count_pct"] = safe_divide(
                ahead.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["even_count_pct"] = safe_divide(
                even.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["behind_in_count_pct"] = safe_divide(
                behind.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["two_strike_pa_pct"] = safe_divide(
                two_strike_pa.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["three_ball_pa_pct"] = safe_divide(
                three_ball_pa.reindex(player_index, fill_value=0), pa_counts
            )

            if {
                "inning",
                "bat_score",
                "fld_score",
            }.issubset(pa_last.columns):
                inning = pd.to_numeric(pa_last["inning"], errors="coerce")
                bat_score = pd.to_numeric(pa_last["bat_score"], errors="coerce")
                fld_score = pd.to_numeric(pa_last["fld_score"], errors="coerce")
                close_mask = (inning >= 7) & (bat_score - fld_score).abs().le(1)
                late_close = close_mask.groupby(pa_last["player_id"]).sum()
                metrics["late_close_pa"] = late_close.reindex(
                    player_index, fill_value=0
                )

            alignment = None
            if "if_fielding_alignment" in pa_last.columns:
                alignment = pa_last["if_fielding_alignment"]
            elif "of_fielding_alignment" in pa_last.columns:
                alignment = pa_last["of_fielding_alignment"]
            if alignment is not None:
                shifted = alignment.notna() & (alignment != "Standard")
                shifted_count = shifted.groupby(pa_last["player_id"]).sum()
                non_shifted_count = (~shifted).groupby(pa_last["player_id"]).sum()
                metrics["shifted_pa_pct"] = safe_divide(
                    shifted_count.reindex(player_index, fill_value=0), pa_counts
                )
                metrics["non_shifted_pa_pct"] = safe_divide(
                    non_shifted_count.reindex(player_index, fill_value=0), pa_counts
                )

            if "delta_home_win_exp" in pa_last.columns:
                delta_we = pd.to_numeric(
                    pa_last["delta_home_win_exp"], errors="coerce"
                ).abs()
                metrics["pli"] = delta_we.groupby(pa_last["player_id"]).mean()

    if "launch_speed" in statcast_df.columns:
        launch_speed = pd.to_numeric(statcast_df["launch_speed"], errors="coerce")
        batted_mask = launch_speed.notna()
        batted_speed = statcast_df.loc[batted_mask].copy()
        batted_speed["launch_speed"] = launch_speed[batted_mask]

        speed_group = batted_speed.groupby("player_id")["launch_speed"]
        metrics["ev"] = speed_group.mean()
        metrics["maxev"] = speed_group.max()
        metrics["median_ev"] = speed_group.median()
        metrics["ev_p10"] = speed_group.quantile(0.1)
        metrics["ev_p50"] = speed_group.quantile(0.5)
        metrics["ev_p90"] = speed_group.quantile(0.9)

        hard_hit = batted_speed.loc[
            batted_speed["launch_speed"] >= 95
        ].groupby("player_id").size()
        batted_counts = batted_speed.groupby("player_id").size()
        metrics["hardhitpct"] = safe_divide(hard_hit, batted_counts)

        if "launch_angle" in statcast_df.columns:
            launch_angle = pd.to_numeric(statcast_df["launch_angle"], errors="coerce")
            angle_mask = launch_angle.notna()
            batted_angle = statcast_df.loc[angle_mask].copy()
            batted_angle["launch_angle"] = launch_angle[angle_mask]
            metrics["la"] = batted_angle.groupby("player_id")[
                "launch_angle"
            ].mean()
            metrics["la_sd"] = batted_angle.groupby("player_id")[
                "launch_angle"
            ].std(ddof=0)

            sweet_spot_mask = launch_angle.between(8, 32)
            sweet_spot = statcast_df.loc[
                sweet_spot_mask & angle_mask
            ].groupby("player_id").size()
            angles = statcast_df.loc[angle_mask].groupby("player_id").size()
            metrics["sweet_spot_pct"] = safe_divide(sweet_spot, angles)

        if "launch_speed_angle" in statcast_df.columns:
            lsa = pd.to_numeric(
                statcast_df["launch_speed_angle"], errors="coerce"
            )
            lsa_mask = lsa.notna()
            lsa_counts = statcast_df.loc[lsa_mask].groupby("player_id").size()
            barrels = statcast_df.loc[lsa == 6].groupby("player_id").size()
            under = statcast_df.loc[lsa == 3].groupby("player_id").size()
            flare = statcast_df.loc[lsa == 4].groupby("player_id").size()
            poorly_weak = statcast_df.loc[lsa == 1].groupby("player_id").size()
            poorly_topped = statcast_df.loc[lsa == 2].groupby("player_id").size()
            poorly_under = statcast_df.loc[lsa == 3].groupby("player_id").size()
            poorly_hit = statcast_df.loc[lsa.isin([1, 2, 3])].groupby(
                "player_id"
            ).size()

            metrics["barrels"] = barrels
            metrics["barrelpct"] = safe_divide(barrels, lsa_counts)
            metrics["under_pct"] = safe_divide(under, lsa_counts)
            metrics["flare_burner_pct"] = safe_divide(flare, lsa_counts)
            metrics["poorly_weak_pct"] = safe_divide(poorly_weak, lsa_counts)
            metrics["poorly_topped_pct"] = safe_divide(poorly_topped, lsa_counts)
            metrics["poorly_under_pct"] = safe_divide(poorly_under, lsa_counts)
            metrics["poorly_hit_pct"] = safe_divide(poorly_hit, lsa_counts)

            bip_mask = None
            if "type" in statcast_df.columns:
                bip_mask = statcast_df["type"] == "X"
            elif "events" in statcast_df.columns:
                bip_mask = statcast_df["events"].notna()
            else:
                bip_mask = batted_mask

            if bip_mask is not None:
                bip_counts = statcast_df.loc[bip_mask].groupby("player_id").size()
                metrics["barrels_per_bip"] = safe_divide(barrels, bip_counts)

    if "bb_type" in statcast_df.columns:
        bb_type = statcast_df["bb_type"]
        bb_mask = bb_type.notna()
        batted = statcast_df.loc[bb_mask].copy()
        total_batted = batted.groupby("player_id").size().reindex(
            player_index, fill_value=0
        )
        gb = batted.loc[bb_type == "ground_ball"].groupby("player_id").size()
        ld = batted.loc[bb_type == "line_drive"].groupby("player_id").size()
        fb = batted.loc[bb_type == "fly_ball"].groupby("player_id").size()
        pop = batted.loc[bb_type == "popup"].groupby("player_id").size()

        metrics["gbpct"] = safe_divide(gb.reindex(player_index, fill_value=0), total_batted)
        metrics["ldpct"] = safe_divide(ld.reindex(player_index, fill_value=0), total_batted)
        metrics["fbpct"] = safe_divide(fb.reindex(player_index, fill_value=0), total_batted)
        fly_total = fb.reindex(player_index, fill_value=0) + pop.reindex(
            player_index, fill_value=0
        )
        metrics["iffbpct"] = safe_divide(pop.reindex(player_index, fill_value=0), fly_total)

        if pa_counts is not None:
            metrics["gb_per_pa"] = safe_divide(
                gb.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["fb_per_pa"] = safe_divide(
                fb.reindex(player_index, fill_value=0), pa_counts
            )
            metrics["ld_per_pa"] = safe_divide(
                ld.reindex(player_index, fill_value=0), pa_counts
            )

        if {"hc_x", "hc_y", "stand"}.issubset(batted.columns):
            coords = batted.dropna(subset=["hc_x", "hc_y", "stand"]).copy()
            if not coords.empty:
                angle = np.degrees(
                    np.arctan2(coords["hc_x"] - 125.42, 198.27 - coords["hc_y"])
                )
                stand = coords["stand"]
                pull_mask = (stand == "R") & (angle >= 25)
                pull_mask |= (stand == "L") & (angle <= -25)
                oppo_mask = (stand == "R") & (angle <= -25)
                oppo_mask |= (stand == "L") & (angle >= 25)
                center_mask = ~(pull_mask | oppo_mask)

                total_dir = coords.groupby("player_id").size()
                pull = coords.loc[pull_mask].groupby("player_id").size()
                oppo = coords.loc[oppo_mask].groupby("player_id").size()
                center = coords.loc[center_mask].groupby("player_id").size()

                metrics["pullpct"] = safe_divide(
                    pull.reindex(player_index, fill_value=0),
                    total_dir.reindex(player_index, fill_value=0),
                )
                metrics["oppopct"] = safe_divide(
                    oppo.reindex(player_index, fill_value=0),
                    total_dir.reindex(player_index, fill_value=0),
                )
                metrics["centpct"] = safe_divide(
                    center.reindex(player_index, fill_value=0),
                    total_dir.reindex(player_index, fill_value=0),
                )
                metrics["straightaway_pct"] = metrics["centpct"]

                air_mask = coords["bb_type"].isin(
                    ["fly_ball", "line_drive", "popup"]
                )
                air_total = coords.loc[air_mask].groupby("player_id").size()
                pull_air = coords.loc[air_mask & pull_mask].groupby("player_id").size()
                oppo_air = coords.loc[air_mask & oppo_mask].groupby("player_id").size()
                metrics["pull_air_pct"] = safe_divide(
                    pull_air.reindex(player_index, fill_value=0),
                    air_total.reindex(player_index, fill_value=0),
                )
                metrics["oppo_air_pct"] = safe_divide(
                    oppo_air.reindex(player_index, fill_value=0),
                    air_total.reindex(player_index, fill_value=0),
                )

    if "events" in statcast_df.columns:
        events = statcast_df["events"]
        ab_counts = events.loc[
            events.notna() & ~events.isin(NON_AB_EVENTS)
        ].groupby(statcast_df["player_id"]).size()
        bb_counts = events.loc[
            events.isin(WALK_EVENTS)
        ].groupby(statcast_df["player_id"]).size()
        hbp_counts = events.loc[
            events.isin(HBP_EVENTS)
        ].groupby(statcast_df["player_id"]).size()
        sf_counts = events.loc[
            events.isin(SAC_FLY_EVENTS)
        ].groupby(statcast_df["player_id"]).size()

        ab_counts = ab_counts.reindex(player_index, fill_value=0)
        bb_counts = bb_counts.reindex(player_index, fill_value=0)
        hbp_counts = hbp_counts.reindex(player_index, fill_value=0)
        sf_counts = sf_counts.reindex(player_index, fill_value=0)

        expected_hits = None
        if "estimated_ba_using_speedangle" in statcast_df.columns:
            expected_ba = pd.to_numeric(
                statcast_df["estimated_ba_using_speedangle"], errors="coerce"
            ).fillna(0)
            expected_hits = expected_ba.groupby(statcast_df["player_id"]).sum(
                min_count=1
            ).reindex(player_index, fill_value=0)
            metrics["xba"] = safe_divide(expected_hits, ab_counts)

        if "estimated_slg_using_speedangle" in statcast_df.columns:
            expected_slg = pd.to_numeric(
                statcast_df["estimated_slg_using_speedangle"], errors="coerce"
            ).fillna(0)
            expected_tb = expected_slg.groupby(statcast_df["player_id"]).sum(
                min_count=1
            ).reindex(player_index, fill_value=0)
            metrics["xslg"] = safe_divide(expected_tb, ab_counts)

        if expected_hits is not None:
            xobp_numer = expected_hits + bb_counts + hbp_counts
            xobp_denom = ab_counts + bb_counts + hbp_counts + sf_counts
            metrics["xobp"] = safe_divide(xobp_numer, xobp_denom)

    if "woba_denom" in statcast_df.columns:
        woba_denom = pd.to_numeric(statcast_df["woba_denom"], errors="coerce").fillna(0)
        woba_value = pd.to_numeric(
            statcast_df.get("woba_value"), errors="coerce"
        )
        xwoba_est = pd.to_numeric(
            statcast_df.get("estimated_woba_using_speedangle"), errors="coerce"
        )
        xwoba_value = xwoba_est.where(xwoba_est.notna(), woba_value)
        valid_mask = woba_denom > 0
        xwoba_value = xwoba_value.where(valid_mask)
        woba_denom = woba_denom.where(valid_mask, 0)

        xwoba_sum = xwoba_value.groupby(statcast_df["player_id"]).sum(min_count=1)
        denom_sum = woba_denom.groupby(statcast_df["player_id"]).sum()
        metrics["xwoba"] = safe_divide(xwoba_sum, denom_sum)

    metrics = metrics.reset_index()
    for col in STATCAST_BATTER_COLUMNS:
        if col not in metrics.columns:
            metrics[col] = pd.NA
    return metrics


def build_statcast_pitcher_metrics_from_df(
    statcast_df: pd.DataFrame,
) -> pd.DataFrame:
    if "pitcher" not in statcast_df.columns:
        return pd.DataFrame(columns=["player_id"] + STATCAST_PITCHER_COLUMNS)

    statcast_df = statcast_df.copy()
    statcast_df["player_id"] = pd.to_numeric(
        statcast_df["pitcher"], errors="coerce"
    )
    statcast_df = statcast_df[statcast_df["player_id"].notna()].copy()
    if statcast_df.empty:
        return pd.DataFrame(columns=["player_id"] + STATCAST_PITCHER_COLUMNS)

    statcast_df["player_id"] = statcast_df["player_id"].astype(int)
    player_index = pd.Index(
        statcast_df["player_id"].dropna().unique(), name="player_id"
    )
    metrics = pd.DataFrame(index=player_index)

    pitch_type = statcast_df.get("pitch_type")
    total_pitches = statcast_df.groupby("player_id").size()

    if pitch_type is not None:
        pitch_type = pitch_type.fillna("UNK")
        all_known = (
            FASTBALL_TYPES
            | SLIDER_TYPES
            | CUTTER_TYPES
            | CURVEBALL_TYPES
            | CHANGEUP_TYPES
            | SPLITTER_TYPES
            | KNUCKLE_TYPES
        )

        def usage(mask):
            counts = statcast_df.loc[mask].groupby("player_id").size()
            return safe_divide(
                counts.reindex(player_index, fill_value=0),
                total_pitches.reindex(player_index, fill_value=0),
            )

        metrics["fbpct_2"] = usage(pitch_type.isin(FASTBALL_TYPES))
        metrics["slpct"] = usage(pitch_type.isin(SLIDER_TYPES))
        metrics["ctpct"] = usage(pitch_type.isin(CUTTER_TYPES))
        metrics["cbpct"] = usage(pitch_type.isin(CURVEBALL_TYPES))
        metrics["chpct"] = usage(pitch_type.isin(CHANGEUP_TYPES))
        metrics["sfpct"] = usage(pitch_type.isin(SPLITTER_TYPES))
        metrics["knpct"] = usage(pitch_type.isin(KNUCKLE_TYPES))
        metrics["xxpct"] = usage(~pitch_type.isin(all_known))

    if "release_speed" in statcast_df.columns:
        release_speed = pd.to_numeric(
            statcast_df["release_speed"], errors="coerce"
        )
        metrics["avg_velo"] = release_speed.groupby(statcast_df["player_id"]).mean()
        metrics["max_velo"] = release_speed.groupby(statcast_df["player_id"]).max()
        metrics["velo_sd"] = release_speed.groupby(statcast_df["player_id"]).std(
            ddof=0
        )

        def velo_by_type(type_set):
            mask = pitch_type.isin(type_set) if pitch_type is not None else False
            subset = release_speed.where(mask)
            return subset.groupby(statcast_df["player_id"]).mean()

        metrics["fbv"] = velo_by_type(FASTBALL_TYPES)
        metrics["slv"] = velo_by_type(SLIDER_TYPES)
        metrics["ctv"] = velo_by_type(CUTTER_TYPES)
        metrics["cbv"] = velo_by_type(CURVEBALL_TYPES)
        metrics["chv"] = velo_by_type(CHANGEUP_TYPES)
        metrics["sfv"] = velo_by_type(SPLITTER_TYPES)
        metrics["knv"] = velo_by_type(KNUCKLE_TYPES)

    if "release_spin_rate" in statcast_df.columns:
        spin = pd.to_numeric(
            statcast_df["release_spin_rate"], errors="coerce"
        )
        metrics["spin_rate"] = spin.groupby(statcast_df["player_id"]).mean()
        metrics["spin_sd"] = spin.groupby(statcast_df["player_id"]).std(ddof=0)

    if "spin_axis" in statcast_df.columns:
        axis = pd.to_numeric(statcast_df["spin_axis"], errors="coerce")
        metrics["spin_axis"] = axis.groupby(statcast_df["player_id"]).mean()

    if "release_extension" in statcast_df.columns:
        extension = pd.to_numeric(
            statcast_df["release_extension"], errors="coerce"
        )
        metrics["extension"] = extension.groupby(statcast_df["player_id"]).mean()

    if "release_pos_z" in statcast_df.columns:
        release_z = pd.to_numeric(statcast_df["release_pos_z"], errors="coerce")
        metrics["release_height"] = release_z.groupby(statcast_df["player_id"]).mean()

    if "release_pos_x" in statcast_df.columns:
        release_x = pd.to_numeric(statcast_df["release_pos_x"], errors="coerce")
        metrics["release_side"] = release_x.groupby(statcast_df["player_id"]).mean()

    metrics = metrics.reset_index()
    for col in STATCAST_PITCHER_COLUMNS:
        if col not in metrics.columns:
            metrics[col] = pd.NA
    return metrics
