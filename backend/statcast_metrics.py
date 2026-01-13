from __future__ import annotations

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
    "under_pct",
    "flare_burner_pct",
    "poorly_hit_pct",
    "poorly_under_pct",
    "poorly_topped_pct",
    "poorly_weak_pct",
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

STATCAST_REQUIRED_COLUMNS = {
    "player_id",
    "description",
    "zone",
    "pitch_number",
    "strikes",
    "launch_speed",
    "launch_angle",
    "launch_speed_angle",
    "type",
    "events",
}


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

    metrics = metrics.reset_index()
    for col in STATCAST_BATTER_COLUMNS:
        if col not in metrics.columns:
            metrics[col] = pd.NA
    return metrics
