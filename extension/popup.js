const yearSelect = document.getElementById("year");
const searchInput = document.getElementById("search");
const searchButton = document.getElementById("search-btn");
const clearButton = document.getElementById("clear-btn");
const browseTeamsButton = document.getElementById("browse-teams-btn");
const clearSavedPlayersButton = document.getElementById("clear-saved-players");
const clearSavedPlayersCompareButton = document.getElementById(
  "clear-saved-players-compare"
);
const rangeStartInput = document.getElementById("range-start");
const rangeEndInput = document.getElementById("range-end");
const rangeEnabledInput = document.getElementById("range-enabled");
const resultsEl = document.getElementById("results");
const savedPlayersEl = document.getElementById("saved-players");
const savedPlayersCompareEl = document.getElementById("saved-players-compare");
const teamsListEl = document.getElementById("teams-list");
const teamHittersEl = document.getElementById("team-hitters");
const teamPitchersEl = document.getElementById("team-pitchers");
const teamTitleEl = document.getElementById("team-title");
const selectedStatsPlayersEl = document.getElementById("selected-stats-players");
const selectedStatsCompareEl = document.getElementById("selected-stats-compare");
const viewButton = document.getElementById("view-btn");
const compareButton = document.getElementById("compare-run-btn");
const statsEl = document.getElementById("stats");
const outputPlayer = document.getElementById("output-player");
const outputCompare = document.getElementById("output-compare");
const downloadPlayerButton = document.getElementById("download-player");
const downloadCompareButton = document.getElementById("download-compare");
const metaEl = document.getElementById("snapshot-meta");
const warningEl = document.getElementById("snapshot-warning");
const statsLimitEl = document.getElementById("stats-limit");
const statsRangeWarningEl = document.getElementById("stats-range-warning");
const statsCountEl = document.getElementById("stats-count-players");
const playersLimitEl = document.getElementById("players-limit");
const compareLimitEl = document.getElementById("compare-limit");
const chosenPlayerEl = document.getElementById("chosen-player");
const modeSelect = document.getElementById("mode");
const tabPlayers = document.getElementById("tab-players");
const tabCompare = document.getElementById("tab-compare");
const tabTeams = document.getElementById("tab-teams");
const tabStats = document.getElementById("tab-stats");
const tabLeaderboard = document.getElementById("tab-leaderboard");
const panelPlayers = document.getElementById("panel-players");
const panelCompare = document.getElementById("panel-compare");
const panelTeams = document.getElementById("panel-teams");
const panelStats = document.getElementById("panel-stats");
const panelLeaderboard = document.getElementById("panel-leaderboard");
const statSearchInput = document.getElementById("stat-search");
const statSearchResults = document.getElementById("stat-search-results");
const leaderboardOutput = document.getElementById("leaderboard-output");
const leaderboardMeta = document.getElementById("leaderboard-meta");
const leaderboardRoleWrap = document.getElementById("leaderboard-role-wrap");
const leaderboardRoleStarters = document.getElementById(
  "leaderboard-role-starters"
);
const leaderboardRoleRelievers = document.getElementById(
  "leaderboard-role-relievers"
);

const stateByMode = {
  hitters: {
    savedPlayers: [],
    activePlayerId: null,
    activeCompareIds: new Set(),
    selectedStatKeys: new Set(),
    statsByKey: new Map(),
    statsConfig: [],
    configLoaded: false,
  },
  pitchers: {
    savedPlayers: [],
    activePlayerId: null,
    activeCompareIds: new Set(),
    selectedStatKeys: new Set(),
    statsByKey: new Map(),
    statsConfig: [],
    configLoaded: false,
  },
};
let activeMode = modeSelect ? modeSelect.value : "hitters";
const snapshotsByYear = new Map();
const pitcherSnapshotsByYear = new Map();
const rangeCache = new Map();
let activePlayers = [];
let activePitchers = [];
const activeMetaByMode = { hitters: null, pitchers: null };
let activeTeam = null;
let activeLeaderboardStat = null;
const PITCHER_ROLE_STARTERS = "starters";
const PITCHER_ROLE_RELIEVERS = "relievers";
const DEFAULT_PITCHER_ROLE = PITCHER_ROLE_STARTERS;
const leaderboardStateByMode = {
  hitters: { statKey: null, meta: "", output: "", year: null, pitcherRole: null },
  pitchers: {
    statKey: null,
    meta: "",
    output: "",
    year: null,
    pitcherRole: DEFAULT_PITCHER_ROLE,
  },
};
const SNAPSHOT_BASE_URL =
  "https://cdn.jsdelivr.net/gh/tradedgesystem/mlb-stats-backend@main/extension/snapshots";
const LOCAL_API_BASE = "http://127.0.0.1:8000";
const LOCAL_SNAPSHOT_BASE =
  typeof chrome !== "undefined" && chrome.runtime?.getURL
    ? chrome.runtime.getURL("snapshots")
    : "";
const MAX_STATS = 10;
const MAX_SAVED_PLAYERS = 10;
const MAX_COMPARE_PLAYERS = 5;
const CONFIG_FILES = {
  hitters: "stats_config.json",
  pitchers: "pitching_stats_config.json",
};
const STATCAST_RANGE_KEYS = new Set([
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
  "barrels_per_pa",
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
]);
const STATCAST_RANGE_KEYS_PITCHERS = new Set([
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
]);
const STORAGE_KEY = "mlb_stats_state_v1";
let persistTimer = null;

const persistNow = () => {
  // Check if we're in a Chrome extension context
  if (typeof chrome === 'undefined' || !chrome?.storage?.local) {
    return;
  }
  try {
    const state = serializeState();
    chrome.storage.local.set({ [STORAGE_KEY]: state }, () => {
      if (chrome.runtime?.lastError) {
        console.error("[Persistence] Save failed:", chrome.runtime.lastError);
      }
    });
  } catch (error) {
    // Silently fail in non-extension contexts
  }
};

const getState = () => stateByMode[activeMode];
const getActiveDataset = () =>
  activeMode === "pitchers" ? activePitchers : activePlayers;
const getActiveMeta = () => activeMetaByMode[activeMode];

const serializeModeState = (modeState) => ({
  savedPlayers: modeState.savedPlayers,
  activePlayerId: modeState.activePlayerId,
  activeCompareIds: Array.from(modeState.activeCompareIds),
  selectedStatKeys: Array.from(modeState.selectedStatKeys),
});

const serializeState = () => ({
  version: 1,
  year: yearSelect ? yearSelect.value : null,
  mode: activeMode,
  rangeEnabled: Boolean(rangeEnabledInput && rangeEnabledInput.checked),
  rangeStart: rangeStartInput ? rangeStartInput.value : "",
  rangeEnd: rangeEndInput ? rangeEndInput.value : "",
  leaderboard: {
    hitters: leaderboardStateByMode.hitters,
    pitchers: leaderboardStateByMode.pitchers,
  },
  modes: {
    hitters: serializeModeState(stateByMode.hitters),
    pitchers: serializeModeState(stateByMode.pitchers),
  },
});

const schedulePersist = () => {
  // Check if we're in a Chrome extension context
  if (typeof chrome === 'undefined' || !chrome?.storage?.local) {
    return;
  }
  if (persistTimer) {
    clearTimeout(persistTimer);
  }
  persistTimer = setTimeout(() => {
    try {
      const state = serializeState();
      chrome.storage.local.set({ [STORAGE_KEY]: state }, () => {
        if (chrome.runtime?.lastError) {
          console.error("[Persistence] Save failed:", chrome.runtime.lastError);
        }
      });
    } catch (error) {
      // Silently fail in non-extension contexts
    }
  }, 50);
};

const restoreModeState = (mode, saved) => {
  const state = stateByMode[mode];
  if (!state || !saved) {
    return;
  }
  state.savedPlayers.length = 0;
  if (Array.isArray(saved.savedPlayers)) {
    state.savedPlayers.push(...saved.savedPlayers);
  }
  state.activePlayerId =
    typeof saved.activePlayerId === "number" ? saved.activePlayerId : null;
  state.activeCompareIds.clear();
  if (Array.isArray(saved.activeCompareIds)) {
    saved.activeCompareIds.forEach((id) => {
      if (typeof id === "number") {
        state.activeCompareIds.add(id);
      }
    });
  }
  state.selectedStatKeys.clear();
  if (Array.isArray(saved.selectedStatKeys)) {
    saved.selectedStatKeys.forEach((key) => {
      if (typeof key === "string") {
        state.selectedStatKeys.add(key);
      }
    });
  }
};

const normalizeLeaderboardState = (saved) => {
  if (!saved) {
    return { statKey: null, meta: "", output: "", year: null, pitcherRole: null };
  }
  if (typeof saved === "string") {
    return { statKey: saved, meta: "", output: "", year: null, pitcherRole: null };
  }
  if (typeof saved === "object") {
    return {
      statKey: typeof saved.statKey === "string" ? saved.statKey : null,
      meta: typeof saved.meta === "string" ? saved.meta : "",
      output: typeof saved.output === "string" ? saved.output : "",
      year: typeof saved.year === "string" ? saved.year : null,
      pitcherRole:
        typeof saved.pitcherRole === "string" ? saved.pitcherRole : null,
    };
  }
  return { statKey: null, meta: "", output: "", year: null, pitcherRole: null };
};

const normalizePitcherRole = (role) => {
  if (role === PITCHER_ROLE_STARTERS || role === PITCHER_ROLE_RELIEVERS) {
    return role;
  }
  return DEFAULT_PITCHER_ROLE;
};

const getActivePitcherRole = () =>
  normalizePitcherRole(leaderboardStateByMode.pitchers.pitcherRole);

const getActivePitcherRoleLabel = () =>
  getActivePitcherRole() === PITCHER_ROLE_STARTERS ? "Starters" : "Relievers";

const loadPersistedState = async () => {
  // Check if we're in a Chrome extension context
  if (typeof chrome === 'undefined' || !chrome?.storage?.local) {
    return;
  }
  try {
    const stored = await new Promise((resolve, reject) => {
      chrome.storage.local.get(STORAGE_KEY, (result) => {
        if (chrome.runtime?.lastError) {
          reject(chrome.runtime.lastError);
        } else {
          resolve(result);
        }
      });
    });
    const payload = stored ? stored[STORAGE_KEY] : null;
    if (!payload || payload.version !== 1) {
      return;
    }
    if (payload.year && yearSelect) {
      yearSelect.value = payload.year;
    }
    if (payload.mode && modeSelect) {
      modeSelect.value = payload.mode;
      activeMode = payload.mode;
    }
    if (rangeEnabledInput && typeof payload.rangeEnabled === "boolean") {
      rangeEnabledInput.checked = payload.rangeEnabled;
    }
    if (rangeStartInput && typeof payload.rangeStart === "string") {
      rangeStartInput.value = payload.rangeStart;
    }
    if (rangeEndInput && typeof payload.rangeEnd === "string") {
      rangeEndInput.value = payload.rangeEnd;
    }
    restoreModeState("hitters", payload.modes?.hitters);
    restoreModeState("pitchers", payload.modes?.pitchers);
    const savedLeaderboard = payload.leaderboard || {};
    leaderboardStateByMode.hitters = normalizeLeaderboardState(
      savedLeaderboard.hitters
    );
    leaderboardStateByMode.pitchers = normalizeLeaderboardState(
      savedLeaderboard.pitchers
    );
    leaderboardStateByMode.pitchers.pitcherRole = normalizePitcherRole(
      leaderboardStateByMode.pitchers.pitcherRole
    );
    activeLeaderboardStat = leaderboardStateByMode[activeMode].statKey || null;
  } catch (error) {
    // Silently fail in non-extension contexts or on errors
  }
};

const getSelectedKeys = () => {
  const { statsConfig, selectedStatKeys } = getState();
  if (!statsConfig.length) {
    return [];
  }
  return Array.from(selectedStatKeys);
};

const formatValue = (value, format) => {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "number" || Number.isNaN(value)) {
    return value;
  }
  switch (format) {
    case "integer":
      return String(Math.round(value));
    case "float":
      return value.toFixed(1);
    case "rate":
      return value.toFixed(3);
    case "percent":
      return `${(value * 100).toFixed(1)}%`;
    default:
      return value;
  }
};

const isRangeMode = () => Boolean(rangeEnabledInput && rangeEnabledInput.checked);

const isRangeSupported = (key) => {
  const { statsByKey } = getState();
  const config = statsByKey.get(key);
  return Boolean(config && config.range_supported);
};

const rangeNeedsStatcast = (statKeys) => {
  if (activeMode === "hitters") {
    return statKeys.some((key) => STATCAST_RANGE_KEYS.has(key));
  }
  if (activeMode === "pitchers") {
    return statKeys.some((key) => STATCAST_RANGE_KEYS_PITCHERS.has(key));
  }
  return false;
};

const isStatcastKeyForMode = (key) => {
  if (activeMode === "hitters") {
    return STATCAST_RANGE_KEYS.has(key);
  }
  if (activeMode === "pitchers") {
    return STATCAST_RANGE_KEYS_PITCHERS.has(key);
  }
  return false;
};

const getSeasonFallbackRange = (year) => ({
  start: `${year}-03-01`,
  end: `${year}-11-30`,
});

const getRangeParams = () => {
  if (!rangeStartInput || !rangeEndInput) {
    return null;
  }
  const start = rangeStartInput.value;
  const end = rangeEndInput.value;
  if (!start || !end) {
    return null;
  }
  return { start, end };
};

const fetchRangeRows = async (playerIds, statKeys, overrideRange = null) => {
  const params = overrideRange || getRangeParams();
  if (!params) {
    throw new Error("Select a start and end date for range mode.");
  }
  if (!yearSelect) {
    throw new Error("Select a season year first.");
  }
  const includeStatcast = rangeNeedsStatcast(statKeys);
  const cacheKey = [
    activeMode,
    yearSelect.value,
    params.start,
    params.end,
    includeStatcast ? "statcast" : "basic",
    playerIds.join(","),
  ].join("|");
  if (rangeCache.has(cacheKey)) {
    return rangeCache.get(cacheKey);
  }
  const query = new URLSearchParams({
    year: yearSelect.value,
    start: params.start,
    end: params.end,
    player_ids: playerIds.join(","),
  });
  if (includeStatcast) {
    query.set("include_statcast", "true");
  }
  const endpoint = activeMode === "pitchers" ? "pitchers" : "players";
  const response = await fetch(`${LOCAL_API_BASE}/${endpoint}/range?${query}`);
  if (!response.ok) {
    throw new Error(`Range API failed: ${response.status}`);
  }
  const data = await response.json();
  rangeCache.set(cacheKey, data);
  return data;
};

const leaderboardStatcastCache = new Map();

const shouldUseStatcastLeaderboard = (statKey, dataset) => {
  const statcastKeys =
    activeMode === "pitchers"
      ? STATCAST_RANGE_KEYS_PITCHERS
      : STATCAST_RANGE_KEYS;
  if (!statcastKeys.has(statKey)) {
    return false;
  }
  const hasValue = dataset.some((player) => {
    const value = player[statKey];
    return value !== null && value !== undefined && !Number.isNaN(value);
  });
  return !hasValue;
};

const fetchStatcastLeaderboard = async (statKey) => {
  if (!yearSelect) {
    throw new Error("Select a season year first.");
  }
  const cacheKey = [activeMode, yearSelect.value, statKey].join("|");
  if (leaderboardStatcastCache.has(cacheKey)) {
    return leaderboardStatcastCache.get(cacheKey);
  }
  const query = new URLSearchParams({
    year: yearSelect.value,
    mode: activeMode,
    stat_key: statKey,
  });
  const response = await fetch(
    `${LOCAL_API_BASE}/leaderboard/statcast?${query}`
  );
  if (!response.ok) {
    throw new Error(`Leaderboard API failed: ${response.status}`);
  }
  const data = await response.json();
  leaderboardStatcastCache.set(cacheKey, data);
  return data;
};

const mergeRangeRowsForKeys = (playerIds, rangeRows, seasonRows, statKeys) => {
  const rangeById = new Map(rangeRows.map((row) => [row.player_id, row]));
  const seasonById = new Map(seasonRows.map((row) => [row.player_id, row]));
  return playerIds
    .map((playerId) => {
      const seasonRow = seasonById.get(playerId);
      const rangeRow = rangeById.get(playerId);
      if (!seasonRow && !rangeRow) {
        return null;
      }
      if (!seasonRow) {
        return rangeRow;
      }
      if (!rangeRow) {
        return seasonRow;
      }
      const merged = { ...seasonRow };
      statKeys.forEach((key) => {
        if (rangeRow[key] !== undefined) {
          merged[key] = rangeRow[key];
        }
      });
      return merged;
    })
    .filter(Boolean);
};

const mergeRangeRows = (playerIds, rangeRows, seasonRows) => {
  const rangeById = new Map(rangeRows.map((row) => [row.player_id, row]));
  const seasonById = new Map(seasonRows.map((row) => [row.player_id, row]));
  return playerIds
    .map((playerId) => {
      const seasonRow = seasonById.get(playerId);
      const rangeRow = rangeById.get(playerId);
      if (!seasonRow && !rangeRow) {
        return null;
      }
      return { ...(seasonRow || {}), ...(rangeRow || {}) };
    })
    .filter(Boolean);
};

const setRangeWarning = (message) => {
  if (!statsRangeWarningEl) {
    return;
  }
  statsRangeWarningEl.textContent = message || "";
};

const updateMeta = () => {
  if (!metaEl) {
    return;
  }
  const activeMeta = getActiveMeta();
  if (!activeMeta) {
    metaEl.textContent = "Data updated: unavailable";
    if (warningEl) {
      warningEl.textContent = "";
    }
    return;
  }
  const timestamp = activeMeta.generated_at;
  const source = activeMeta.source;
  const date = timestamp ? new Date(timestamp) : null;
  const readable =
    date && !Number.isNaN(date.getTime()) ? date.toLocaleString() : null;
  if (readable) {
    metaEl.textContent = `Data updated: ${readable}${source ? ` (${source})` : ""}`;
  } else if (source) {
    metaEl.textContent = `Data source: ${source}`;
  } else {
    metaEl.textContent = "Data updated: unavailable";
  }

  if (warningEl) {
    if (!date || Number.isNaN(date.getTime())) {
      warningEl.textContent = "";
      return;
    }
    const ageHours = (Date.now() - date.getTime()) / 36e5;
    if (ageHours > 36) {
      const ageDays = Math.floor(ageHours / 24);
      warningEl.textContent = `Snapshot is ${ageDays} day(s) old.`;
    } else {
      warningEl.textContent = "";
    }
  }
};

const updateStatsLimit = () => {
  enforceRangeSelections();
  const { selectedStatKeys } = getState();
  const count = selectedStatKeys.size;
  const atLimit = count >= MAX_STATS;
  if (statsLimitEl) {
    statsLimitEl.textContent =
      `Selected ${count} / ${MAX_STATS}` + (atLimit ? " (max reached)" : "");
    statsLimitEl.classList.toggle("warning", atLimit);
    statsLimitEl.classList.toggle("count", !atLimit);
  }
  if (statsCountEl) {
    statsCountEl.textContent = `Selected stats: ${count} / ${MAX_STATS}`;
  }
  renderSelectedStats();
};

const enforceRangeSelections = () => {
  if (!isRangeMode()) {
    setRangeWarning("");
    return;
  }
  const { selectedStatKeys } = getState();
  const seasonOnlyKeys = Array.from(selectedStatKeys).filter(
    (key) => !isRangeSupported(key)
  );
  if (seasonOnlyKeys.length) {
    seasonOnlyKeys.forEach((key) => selectedStatKeys.delete(key));
    if (statsEl) {
      seasonOnlyKeys.forEach((key) => {
        const checkbox = statsEl.querySelector(
          `input[type="checkbox"][value="${key}"]`
        );
        if (checkbox) {
          checkbox.checked = false;
        }
      });
    }
    setRangeWarning("Range mode is on; season-only stats were removed.");
    return;
  }
  setRangeWarning("");
};

const updatePlayerLimit = () => {
  const { savedPlayers, activeCompareIds, activePlayerId } = getState();
  if (playersLimitEl) {
    if (savedPlayers.length >= MAX_SAVED_PLAYERS) {
      playersLimitEl.textContent = `Max ${MAX_SAVED_PLAYERS} players saved.`;
    } else {
      playersLimitEl.textContent = "";
    }
  }
  if (compareLimitEl) {
    compareLimitEl.textContent =
      `Selected for compare: ${activeCompareIds.size} / ${MAX_COMPARE_PLAYERS}`;
  }
  if (chosenPlayerEl) {
    const chosen = savedPlayers.find(
      (player) => player.player_id === activePlayerId
    );
    chosenPlayerEl.textContent = chosen
      ? `Chosen for view: ${chosen.name}`
      : "Chosen for view: none";
  }
};

const renderSelectedStats = () => {
  const { selectedStatKeys, statsByKey } = getState();
  const labels = Array.from(selectedStatKeys)
    .map((key) => statsByKey.get(key)?.label || key)
    .sort((a, b) => a.localeCompare(b));
  const text = labels.length ? labels.join(", ") : "No stats selected.";
  if (selectedStatsPlayersEl) {
    selectedStatsPlayersEl.textContent = text;
  }
  if (selectedStatsCompareEl) {
    selectedStatsCompareEl.textContent = text;
  }
};

const renderMessage = (message, target) => {
  if (!target) {
    return;
  }
  target.textContent = message;
};

const normalizeText = (value) =>
  value.toLowerCase().replace(/[^a-z0-9]/g, "");

const levenshtein = (a, b) => {
  if (a === b) {
    return 0;
  }
  if (!a || !b) {
    return Math.max(a.length, b.length);
  }
  const dp = Array.from({ length: a.length + 1 }, () =>
    new Array(b.length + 1).fill(0)
  );
  for (let i = 0; i <= a.length; i += 1) {
    dp[i][0] = i;
  }
  for (let j = 0; j <= b.length; j += 1) {
    dp[0][j] = j;
  }
  for (let i = 1; i <= a.length; i += 1) {
    for (let j = 1; j <= b.length; j += 1) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }
  return dp[a.length][b.length];
};

const fuzzyScore = (name, query) => {
  const normalizedName = normalizeText(name);
  const normalizedQuery = normalizeText(query);
  if (!normalizedQuery) {
    return null;
  }
  if (normalizedName.includes(normalizedQuery)) {
    return 0;
  }
  const parts = name
    .toLowerCase()
    .split(/\s+/)
    .map(normalizeText)
    .filter(Boolean);
  let best = levenshtein(normalizedName, normalizedQuery);
  parts.forEach((part) => {
    best = Math.min(best, levenshtein(part, normalizedQuery));
  });
  return best;
};

const renderTeamRoster = () => {
  if (!teamHittersEl || !teamPitchersEl) {
    return;
  }
  teamHittersEl.textContent = "";
  teamPitchersEl.textContent = "";
  if (!activeTeam) {
    teamHittersEl.textContent = "Select a team.";
    teamPitchersEl.textContent = "";
    if (teamTitleEl) {
      teamTitleEl.textContent = "Hitters";
    }
    return;
  }

  const renderRosterList = (players, container, mode) => {
    container.innerHTML = "";
    if (!players.length) {
      container.textContent =
        mode === "pitchers" ? "No pitchers found." : "No hitters found.";
      return;
    }
    players.forEach((player) => {
      const row = document.createElement("div");
      row.textContent = `${player.name} (${player.team})`;
      const addButton = document.createElement("button");
      addButton.textContent = "Add";
      addButton.addEventListener("click", () => {
        addPlayerToMode(mode, player);
        if (activeMode === mode) {
          renderSelected();
          updatePlayerLimit();
        }
      });
      row.appendChild(addButton);
      container.appendChild(row);
    });
  };

  if (teamTitleEl) {
    teamTitleEl.textContent = `Hitters - ${activeTeam}`;
  }
  const hitters = activePlayers
    .filter((player) => player.team === activeTeam)
    .sort((a, b) => a.name.localeCompare(b.name));
  renderRosterList(hitters, teamHittersEl, "hitters");

  const pitchers = activePitchers
    .filter((player) => player.team === activeTeam)
    .sort((a, b) => a.name.localeCompare(b.name));
  renderRosterList(pitchers, teamPitchersEl, "pitchers");
};

const renderTeamsList = () => {
  if (!teamsListEl) {
    return;
  }
  teamsListEl.innerHTML = "";
  const teams = Array.from(
    new Set(
      activePlayers
        .concat(activePitchers)
        .map((player) => player.team)
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b));
  if (!teams.length) {
    teamsListEl.textContent = "No teams available.";
    return;
  }
  teams.forEach((team) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "player-button";
    button.textContent = team;
    if (team === activeTeam) {
      button.classList.add("selected");
    }
    button.addEventListener("click", () => {
      activeTeam = team;
      renderTeamsList();
      renderTeamRoster();
    });
    teamsListEl.appendChild(button);
  });
};

const escapeHtml = (value) =>
  String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const renderAsciiTable = (rows, statKeys, target) => {
  if (!target) {
    return;
  }
  target.textContent = "";
  if (!rows.length) {
    renderMessage("No data to display.", target);
    return;
  }

  const { statsByKey } = getState();
  const rangeParams = isRangeMode() ? getRangeParams() : null;
  const rangeLabel = (() => {
    if (!rangeParams) {
      return null;
    }
    const formatShort = (value) => {
      const parts = value.split("-");
      if (parts.length !== 3) {
        return value;
      }
      const month = Number(parts[1]);
      const day = Number(parts[2]);
      if (!month || !day) {
        return value;
      }
      return `${month}/${day}`;
    };
    const year = yearSelect ? yearSelect.value : rangeParams.start.slice(0, 4);
    return `${year}: ${formatShort(rangeParams.start)} to ${formatShort(rangeParams.end)}`;
  })();
  const baseHeaders = ["Player", "Team"];
  if (rangeLabel) {
    baseHeaders.push("Range");
  }
  const headers = [...baseHeaders];
  statKeys.forEach((key) => {
    const config = statsByKey.get(key);
    headers.push(config?.label || key);
  });

  const bestByKey = new Map();
  if (rows.length > 1) {
    statKeys.forEach((key) => {
      let best = null;
      const config = statsByKey.get(key);
      const lowerIsBetter = Boolean(config && config.lower_is_better);
      rows.forEach((row) => {
        const value = row[key];
        if (typeof value === "number" && !Number.isNaN(value)) {
          if (best === null) {
            best = value;
          } else if (lowerIsBetter && value < best) {
            best = value;
          } else if (!lowerIsBetter && value > best) {
            best = value;
          }
        }
      });
      if (best !== null) {
        bestByKey.set(key, best);
      }
    });
  }

  const rowsData = rows.map((row) => {
    const baseValues = [row.name, row.team];
    if (rangeLabel) {
      baseValues.push(rangeLabel);
    }
    const base = baseValues.map((value) =>
      value === null || value === undefined ? "-" : String(value)
    );
    const stats = statKeys.map((key) => {
      const config = statsByKey.get(key);
      const raw = row[key];
      const formatted = formatValue(raw, config?.format);
      return {
        text: formatted === null || formatted === undefined ? "-" : String(formatted),
        isBest:
          bestByKey.has(key) &&
          typeof raw === "number" &&
          !Number.isNaN(raw) &&
          raw === bestByKey.get(key),
      };
    });
    return { base, stats };
  });

  const baseCount = baseHeaders.length;
  const widths = headers.map((header, index) => {
    const cellWidths = rowsData.map((row) => {
      if (index < baseCount) {
        return row.base[index].length;
      }
      return row.stats[index - baseCount].text.length;
    });
    return Math.max(header.length, ...cellWidths);
  });

  const pad = (value, width) => value + " ".repeat(Math.max(0, width - value.length));
  const line = `+${widths.map((w) => "-".repeat(w + 2)).join("+")}+`;
  const headerLine =
    "| " +
    headers.map((header, i) => pad(header, widths[i])).join(" | ") +
    " |";
  const bodyLines = rowsData.map((row) => {
    const cells = [];
    row.base.forEach((cell, i) => {
      cells.push(pad(escapeHtml(cell), widths[i]));
    });
    row.stats.forEach((cell, index) => {
      const text = pad(escapeHtml(cell.text), widths[index + baseCount]);
      cells.push(cell.isBest ? `<strong>${text}</strong>` : text);
    });
    return "| " + cells.join(" | ") + " |";
  });

  target.innerHTML = [line, headerLine, line, ...bodyLines, line].join("\n");
};

const slugify = (value) =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

const formatDateStamp = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
};

const downloadAsciiAsPng = (target, filename) => {
  if (!target) {
    return;
  }
  const text = target.textContent.trim();
  if (!text) {
    return;
  }
  const lines = text.split("\n");
  const fontFamily = "Courier New";
  const fontSize = 12;
  const lineHeight = 16;
  const padding = 12;

  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  if (!context) {
    return;
  }
  context.font = `${fontSize}px ${fontFamily}`;
  const maxWidth = Math.max(...lines.map((line) => context.measureText(line).width));

  canvas.width = Math.ceil(maxWidth + padding * 2);
  canvas.height = Math.ceil(lines.length * lineHeight + padding * 2);

  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, canvas.width, canvas.height);
  context.fillStyle = "#111111";
  context.font = `${fontSize}px ${fontFamily}`;

  lines.forEach((line, index) => {
    context.fillText(line, padding, padding + lineHeight * (index + 1) - 4);
  });

  const link = document.createElement("a");
  link.href = canvas.toDataURL("image/png");
  link.download = filename;
  link.click();
};

const renderResults = (players) => {
  if (!players.length) {
    resultsEl.textContent = "No matches.";
    return;
  }

  resultsEl.innerHTML = "";
  players.forEach((player) => {
    const row = document.createElement("div");
    row.textContent = `${player.name} (${player.team})`;
    const addButton = document.createElement("button");
    addButton.textContent = "Add";
    addButton.addEventListener("click", () => {
      addPlayer(player);
      searchInput.value = "";
      resultsEl.textContent = "";
    });
    row.appendChild(addButton);
    resultsEl.appendChild(row);
  });
};

const addPlayerToMode = (mode, player) => {
  const state = stateByMode[mode];
  if (!state) {
    return;
  }
  if (state.savedPlayers.find((item) => item.player_id === player.player_id)) {
    return;
  }
  if (state.savedPlayers.length >= MAX_SAVED_PLAYERS) {
    return;
  }
  state.savedPlayers.push(player);
  if (!state.activePlayerId) {
    state.activePlayerId = player.player_id;
  }
  persistNow();
};

const renderSelectedList = (container) => {
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const { savedPlayers, activeCompareIds, activePlayerId } = getState();
  if (!savedPlayers.length) {
    container.textContent = "No players saved.";
    return;
  }
  const isPlayersTab = container === savedPlayersEl;
  savedPlayers.forEach((player) => {
    const pill = document.createElement("div");
    pill.className = "player-pill";

    const button = document.createElement("button");
    button.type = "button";
    button.className = "player-button";
    button.textContent = `${player.name} (${player.team})`;
    if (
      (isPlayersTab && player.player_id === activePlayerId) ||
      (!isPlayersTab && activeCompareIds.has(player.player_id))
    ) {
      button.classList.add("selected");
    }
    button.addEventListener("click", () => {
      if (isPlayersTab) {
        getState().activePlayerId = player.player_id;
      } else {
        const compareIds = getState().activeCompareIds;
        if (compareIds.has(player.player_id)) {
          compareIds.delete(player.player_id);
        } else {
          if (compareIds.size >= MAX_COMPARE_PLAYERS) {
            return;
          }
          compareIds.add(player.player_id);
        }
      }
      updatePlayerLimit();
      renderSelected();
      persistNow();
    });

    const removeButton = document.createElement("button");
    removeButton.type = "button";
    removeButton.className = "player-remove";
    removeButton.textContent = "x";
    removeButton.addEventListener("click", () => {
      removePlayer(player.player_id);
    });

    pill.appendChild(button);
    pill.appendChild(removeButton);
    container.appendChild(pill);
  });
};

const renderSelected = () => {
  renderSelectedList(savedPlayersEl);
  renderSelectedList(savedPlayersCompareEl);
};

const removePlayer = (playerId) => {
  const { savedPlayers, activeCompareIds } = getState();
  const index = savedPlayers.findIndex((item) => item.player_id === playerId);
  if (index === -1) {
    return;
  }
  savedPlayers.splice(index, 1);
  if (getState().activePlayerId === playerId) {
    getState().activePlayerId = null;
  }
  activeCompareIds.delete(playerId);
  renderSelected();
  updatePlayerLimit();
  persistNow();
};

const clearSavedPlayers = () => {
  const { savedPlayers, activeCompareIds } = getState();
  savedPlayers.length = 0;
  activeCompareIds.clear();
  getState().activePlayerId = null;
  renderSelected();
  updatePlayerLimit();
  persistNow();
};

const addPlayer = (player) => {
  addPlayerToMode(activeMode, player);
  renderSelected();
  updatePlayerLimit();
};

const loadSnapshot = async (year) => {
  if (snapshotsByYear.has(year)) {
    const cached = snapshotsByYear.get(year);
    activePlayers = cached.players;
    activeMetaByMode.hitters = cached.meta;
    activeTeam = null;
    renderTeamsList();
    renderTeamRoster();
    if (activeMode === "hitters") {
      updateMeta();
    }
    return;
  }

  try {
    const remoteUrl = `${SNAPSHOT_BASE_URL}/players_${year}.json`;
    const localUrl = LOCAL_SNAPSHOT_BASE
      ? `${LOCAL_SNAPSHOT_BASE}/players_${year}.json`
      : null;
    const preferLocal = Number(year) === 2025 && localUrl;
    const urls = preferLocal
      ? [
          { url: localUrl, source: "bundled snapshot" },
          { url: remoteUrl, source: "cdn snapshot" },
        ]
      : [
          { url: remoteUrl, source: "cdn snapshot" },
          { url: localUrl, source: "bundled snapshot" },
        ].filter((entry) => entry.url);

    let loaded = false;
    for (const entry of urls) {
      try {
        const response = await fetch(entry.url);
        if (!response.ok) {
          throw new Error(`Snapshot fetch failed: ${response.status}`);
        }
        const data = await response.json();
        const players = Array.isArray(data) ? data : data.players || [];
        const meta = Array.isArray(data) ? null : data.meta || null;
        if (meta) {
          meta.source = entry.source;
        }
        snapshotsByYear.set(year, { players, meta });
        activePlayers = players;
        activeMetaByMode.hitters = meta;
        activeTeam = null;
        loaded = true;
        break;
      } catch (innerError) {
        console.log(innerError);
      }
    }
    if (!loaded) {
      throw new Error("Snapshot fetch failed");
    }
  } catch (error) {
    console.log(error);
    try {
      const response = await fetch(`${LOCAL_API_BASE}/players?year=${year}`);
      if (!response.ok) {
        throw new Error(`Local API fetch failed: ${response.status}`);
      }
      const players = await response.json();
      const meta = { source: "local api" };
      snapshotsByYear.set(year, { players, meta });
      activePlayers = players;
      activeMetaByMode.hitters = meta;
      activeTeam = null;
    } catch (innerError) {
      console.log(innerError);
      activePlayers = [];
      activeMetaByMode.hitters = null;
      activeTeam = null;
    }
  }
  renderTeamsList();
  renderTeamRoster();
  if (activeMode === "hitters") {
    updateMeta();
  }
};

const loadPitcherSnapshot = async (year) => {
  if (pitcherSnapshotsByYear.has(year)) {
    const cached = pitcherSnapshotsByYear.get(year);
    activePitchers = cached.players;
    activeMetaByMode.pitchers = cached.meta;
    renderTeamsList();
    renderTeamRoster();
    if (activeMode === "pitchers") {
      updateMeta();
    }
    return;
  }

  try {
    const remoteUrl = `${SNAPSHOT_BASE_URL}/pitchers_${year}.json`;
    const localUrl = LOCAL_SNAPSHOT_BASE
      ? `${LOCAL_SNAPSHOT_BASE}/pitchers_${year}.json`
      : null;
    const preferLocal = Number(year) === 2025 && localUrl;
    const urls = preferLocal
      ? [
          { url: localUrl, source: "bundled snapshot" },
          { url: remoteUrl, source: "cdn snapshot" },
        ]
      : [
          { url: remoteUrl, source: "cdn snapshot" },
          { url: localUrl, source: "bundled snapshot" },
        ].filter((entry) => entry.url);

    let loaded = false;
    for (const entry of urls) {
      try {
        const response = await fetch(entry.url);
        if (!response.ok) {
          throw new Error(`Snapshot fetch failed: ${response.status}`);
        }
        const data = await response.json();
        const players = Array.isArray(data) ? data : data.players || [];
        const meta = Array.isArray(data) ? null : data.meta || null;
        if (meta) {
          meta.source = entry.source;
        }
        pitcherSnapshotsByYear.set(year, { players, meta });
        activePitchers = players;
        activeMetaByMode.pitchers = meta;
        loaded = true;
        break;
      } catch (innerError) {
        console.log(innerError);
      }
    }
    if (!loaded) {
      throw new Error("Snapshot fetch failed");
    }
  } catch (error) {
    console.log(error);
    try {
      const response = await fetch(`${LOCAL_API_BASE}/pitchers?year=${year}`);
      if (!response.ok) {
        throw new Error(`Local API fetch failed: ${response.status}`);
      }
      const players = await response.json();
      const meta = { source: "local api" };
      pitcherSnapshotsByYear.set(year, { players, meta });
      activePitchers = players;
      activeMetaByMode.pitchers = meta;
    } catch (innerError) {
      console.log(innerError);
      activePitchers = [];
      activeMetaByMode.pitchers = null;
    }
  }
  renderTeamsList();
  renderTeamRoster();
  if (activeMode === "pitchers") {
    updateMeta();
  }
};

const updateRangeTagsVisibility = () => {
  const showTags = isRangeMode();
  statsEl
    .querySelectorAll(".range-tag")
    .forEach((tag) => tag.classList.toggle("hidden", !showTags));
};

const updateRangeAvailability = () => {
  const disableRange = false;
  if (rangeEnabledInput) {
    rangeEnabledInput.disabled = disableRange;
    if (disableRange) {
      rangeEnabledInput.checked = false;
    }
  }
  if (rangeStartInput) {
    rangeStartInput.disabled = disableRange;
  }
  if (rangeEndInput) {
    rangeEndInput.disabled = disableRange;
  }
  if (!isRangeMode()) {
    setRangeWarning("");
  }
  updateRangeTagsVisibility();
};

const renderStatsConfig = () => {
  const { statsConfig, statsByKey, selectedStatKeys } = getState();
  const config = statsConfig;
  statsEl.textContent = "";
  const groupOrder = [];
  const groups = {};

  statsByKey.clear();
  const hasSelection = selectedStatKeys.size > 0;

  config.forEach((item) => {
    statsByKey.set(item.key, item);
    if (!groups[item.group]) {
      groups[item.group] = [];
      groupOrder.push(item.group);
    }
    groups[item.group].push(item);
  });

  groupOrder.forEach((group) => {
    const groupEl = document.createElement("div");
    groupEl.className = "stats-group";

    const titleEl = document.createElement("div");
    titleEl.className = "stats-title";
    titleEl.textContent = group;
    groupEl.appendChild(titleEl);

    groups[group].forEach((item) => {
      const row = document.createElement("label");
      row.className = "stat-item";
      const isAvailable = item.available !== false;
      if (!isAvailable) {
        row.classList.add("stat-item-disabled");
        selectedStatKeys.delete(item.key);
      }

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = item.key;
      checkbox.disabled = !isAvailable;
      // Only use defaults if we have NO selection at all
      // If we have selections, always respect them
      checkbox.checked = isAvailable
        ? selectedStatKeys.has(item.key)
        : false;
      checkbox.addEventListener("change", () => {
        if (!isAvailable) {
          return;
        }
        if (checkbox.checked) {
          if (selectedStatKeys.size >= MAX_STATS) {
            checkbox.checked = false;
            updateStatsLimit();
            return;
          }
          selectedStatKeys.add(item.key);
        } else {
          selectedStatKeys.delete(item.key);
        }
        updateStatsLimit();
        enforceRangeSelections();
        persistNow();
      });

      const textWrap = document.createElement("span");
      textWrap.className = "stat-text";

      const text = document.createElement("span");
      text.textContent = item.label;

      const desc = document.createElement("span");
      desc.className = "stat-desc";
      desc.textContent = item.description || "Definition coming soon.";

      textWrap.appendChild(text);
      textWrap.appendChild(desc);
      if (!isAvailable) {
        const unavailableTag = document.createElement("span");
        unavailableTag.className = "stat-tag stat-unavailable";
        unavailableTag.textContent = "Needs data";
        textWrap.appendChild(unavailableTag);
      } else {
        const tag = document.createElement("span");
        tag.className = `stat-tag range-tag ${
          item.range_supported ? "range-ok" : "season-only"
        }`;
        tag.textContent = item.range_supported ? "Range OK" : "Season only";
        textWrap.appendChild(tag);
      }
      row.appendChild(checkbox);
      row.appendChild(textWrap);
      groupEl.appendChild(row);
    });

    statsEl.appendChild(groupEl);
  });

  updateStatsLimit();
  enforceRangeSelections();
  updateRangeTagsVisibility();
};

const runSearch = ({ showEmptyMessage = true } = {}) => {
  try {
    const query = searchInput.value.trim();
    if (!query) {
      resultsEl.textContent = showEmptyMessage ? "Enter a search term." : "";
      return;
    }

    const normalizedQuery = normalizeText(query);
    if (!normalizedQuery) {
      resultsEl.textContent = showEmptyMessage ? "Enter a search term." : "";
      return;
    }

    const dataset = getActiveDataset();
    const maxDistance =
      normalizedQuery.length <= 3
        ? 2
        : Math.max(2, Math.floor(normalizedQuery.length / 2));

    const matches = dataset
      .map((player) => {
        const normalizedName = normalizeText(player.name || "");
        let score = null;
        if (normalizedName.includes(normalizedQuery)) {
          score = 0;
        } else if (normalizedQuery.length > 2) {
          score = fuzzyScore(player.name, query);
        }
        return score === null ? null : { player, score };
      })
      .filter((entry) => entry && entry.score <= maxDistance)
      .sort((a, b) => a.score - b.score || a.player.name.localeCompare(b.player.name))
      .slice(0, 50)
      .map((entry) => ({
        player_id: entry.player.player_id,
        name: entry.player.name,
        team: entry.player.team,
      }));
    console.log(matches);
    renderResults(matches);
  } catch (error) {
    console.log(error);
  }
};

const searchStats = () => {
  try {
    const query = statSearchInput.value.trim();
    if (!query) {
      clearLeaderboardSelection();
      return;
    }

    const normalizedQuery = normalizeText(query);
    if (!normalizedQuery) {
      statSearchResults.textContent = "";
      return;
    }

    const { statsConfig } = getState();
    if (!statsConfig.length) {
      statSearchResults.textContent = "Stats config not loaded.";
      return;
    }

    const maxDistance =
      normalizedQuery.length <= 3
        ? 2
        : Math.max(2, Math.floor(normalizedQuery.length / 2));

    const matches = statsConfig
      .map((stat) => {
        let score = null;
        const labelNormalized = normalizeText(stat.label || "");
        if (labelNormalized.includes(normalizedQuery)) {
          score = 0;
        } else if (normalizedQuery.length > 2) {
          score = fuzzyScore(stat.label, query);
        }
        return score === null ? null : { stat, score };
      })
      .filter((entry) => entry && entry.score <= maxDistance)
      .sort((a, b) => a.score - b.score || a.stat.label.localeCompare(b.stat.label))
      .slice(0, 25)
      .map((entry) => ({
        key: entry.stat.key,
        label: entry.stat.label,
        description: entry.stat.description,
      }));

    renderStatSearchResults(matches);
  } catch (error) {
    console.log(error);
  }
};

const renderStatSearchResults = (matches) => {
  if (!statSearchResults) {
    return;
  }
  statSearchResults.textContent = "";
  if (!matches.length) {
    statSearchResults.textContent = "No matching stats found.";
    return;
  }

  matches.forEach((match) => {
    const row = document.createElement("div");
    row.className = "stat-result";
    row.innerHTML = `<strong>${escapeHtml(match.label)}</strong><br>${escapeHtml(match.description || "")}`;
    row.addEventListener("click", () => {
      selectLeaderboardStat(match.key);
    });
    statSearchResults.appendChild(row);
  });
};

const updateLeaderboardRoleUI = () => {
  if (!leaderboardRoleWrap) {
    return;
  }
  const showRoles = activeMode === "pitchers";
  leaderboardRoleWrap.classList.toggle("hidden", !showRoles);
  if (!showRoles) {
    return;
  }
  const role = getActivePitcherRole();
  if (leaderboardRoleStarters) {
    leaderboardRoleStarters.classList.toggle(
      "selected",
      role === PITCHER_ROLE_STARTERS
    );
  }
  if (leaderboardRoleRelievers) {
    leaderboardRoleRelievers.classList.toggle(
      "selected",
      role === PITCHER_ROLE_RELIEVERS
    );
  }
};

const updateLeaderboardMeta = (statConfig) => {
  if (!leaderboardMeta || !statConfig) {
    return;
  }
  if (activeMode === "pitchers") {
    leaderboardMeta.textContent = `Showing top 25 ${getActivePitcherRoleLabel()} for ${statConfig.label}`;
    return;
  }
  leaderboardMeta.textContent = `Showing top 25 for ${statConfig.label}`;
};

const setPitcherLeaderboardRole = (role) => {
  leaderboardStateByMode.pitchers = {
    ...leaderboardStateByMode.pitchers,
    pitcherRole: normalizePitcherRole(role),
  };
  updateLeaderboardRoleUI();
  if (activeMode !== "pitchers") {
    persistNow();
    return;
  }
  if (activeLeaderboardStat) {
    void renderLeaderboard(activeLeaderboardStat);
    return;
  }
  persistNow();
};

const getPitcherRole = (player) => {
  const g = typeof player.g === "number" && !Number.isNaN(player.g) ? player.g : null;
  const gs = typeof player.gs === "number" && !Number.isNaN(player.gs) ? player.gs : null;
  if (g && gs !== null) {
    return gs / g >= 0.5 ? PITCHER_ROLE_STARTERS : PITCHER_ROLE_RELIEVERS;
  }
  const ip = typeof player.ip === "number" && !Number.isNaN(player.ip) ? player.ip : null;
  if (g && ip !== null) {
    return ip / g >= 3 ? PITCHER_ROLE_STARTERS : PITCHER_ROLE_RELIEVERS;
  }
  return null;
};

const matchesPitcherRole = (player, role) =>
  getPitcherRole(player) === role;

const selectLeaderboardStat = (statKey) => {
  const { statsByKey } = getState();
  const statConfig = statsByKey.get(statKey);
  
  if (!statConfig) {
    console.error("Stat config not found for key:", statKey);
    return;
  }

  activeLeaderboardStat = statKey;
  leaderboardStateByMode[activeMode] = {
    ...leaderboardStateByMode[activeMode],
    statKey,
  };
  resetLeaderboardSearch();
  updateLeaderboardMeta(statConfig);

  void renderLeaderboard(statKey);
};

const renderLeaderboard = async (statKey) => {
  if (!leaderboardOutput) {
    return;
  }
  leaderboardOutput.textContent = "";

  const { statsByKey } = getState();
  const statConfig = statsByKey.get(statKey);
  
  if (!statConfig) {
    renderMessage("Stat configuration not found.", leaderboardOutput);
    persistLeaderboardState(statKey);
    return;
  }
  updateLeaderboardMeta(statConfig);

  let dataset = getActiveDataset();
  if (shouldUseStatcastLeaderboard(statKey, dataset)) {
    try {
      dataset = await fetchStatcastLeaderboard(statKey);
    } catch (error) {
      console.error("[Leaderboard] Statcast fetch failed:", error);
      renderMessage(
        "Failed to fetch leaderboard data. Make sure the local API is running.",
        leaderboardOutput
      );
      persistLeaderboardState(statKey);
      return;
    }
  }

  if (!dataset.length) {
    renderMessage("No data available for this season.", leaderboardOutput);
    persistLeaderboardState(statKey);
    return;
  }

  const hasGames = dataset.some(
    (player) => typeof player.g === "number" && !Number.isNaN(player.g)
  );
  const hasPa = dataset.some(
    (player) => typeof player.pa === "number" && !Number.isNaN(player.pa)
  );
  const hasIp = dataset.some(
    (player) => typeof player.ip === "number" && !Number.isNaN(player.ip)
  );

  const basePlayers = dataset.filter((player) => {
    const value = player[statKey];
    if (value === null || value === undefined || Number.isNaN(value)) {
      return false;
    }

    if (!player.team || player.team.trim() === "") {
      return false;
    }

    if (player.qual === false || player.qual === 0) {
      return false;
    }

    return true;
  });

  const role = activeMode === "pitchers" ? getActivePitcherRole() : null;
  const rolePlayers =
    activeMode === "pitchers"
      ? basePlayers.filter((player) => matchesPitcherRole(player, role))
      : basePlayers;

  const qualifiedPlayers = rolePlayers.filter((player) => {
    if (activeMode === "hitters") {
      if (hasPa) {
        const pa = player.pa;
        if (pa === null || pa === undefined || Number.isNaN(pa) || pa < 150) {
          return false;
        }
      }
      if (hasGames) {
        const g = player.g;
        if (g === null || g === undefined || Number.isNaN(g) || g < 20) {
          return false;
        }
      }
    }

    if (activeMode === "pitchers") {
      if (hasIp) {
        const ip = player.ip;
        if (ip === null || ip === undefined || Number.isNaN(ip) || ip < 50) {
          return false;
        }
      }
      if (hasGames) {
        const g = player.g;
        if (g === null || g === undefined || Number.isNaN(g) || g < 10) {
          return false;
        }
      }
    }

    return true;
  });

  const validPlayers =
    qualifiedPlayers.length >= 25 ? qualifiedPlayers : rolePlayers;

  if (!validPlayers.length) {
    if (activeMode === "pitchers") {
      renderMessage(
        `No qualified ${getActivePitcherRoleLabel().toLowerCase()} have data for this stat.`,
        leaderboardOutput
      );
    } else {
      renderMessage(
        "No qualified MLB players have data for this stat.",
        leaderboardOutput
      );
    }
    persistLeaderboardState(statKey);
    return;
  }

  // Sort appropriately
  const lowerIsBetter = Boolean(statConfig && statConfig.lower_is_better);
  const sortedPlayers = [...validPlayers].sort((a, b) => {
    const valA = a[statKey];
    const valB = b[statKey];
    if (lowerIsBetter) {
      return valA - valB;
    }
    return valB - valA;
  });

  // Take top 25
  const top25 = sortedPlayers.slice(0, 25);

  // Render table with rank
  const headers = ["Rank", "Player", "Team", "Season", statConfig.label];
  const rowsData = top25.map((player, index) => {
    const base = [
      String(index + 1),
      player.name,
      player.team,
      String(player.season),
      formatValue(player[statKey], statConfig.format) || "-",
    ];
    return { base };
  });

  const widths = headers.map((header, index) => {
    const cellWidths = rowsData.map((row) => {
      return row.base[index].length;
    });
    return Math.max(header.length, ...cellWidths);
  });

  const pad = (value, width) => value + " ".repeat(Math.max(0, width - value.length));
  const line = `+${widths.map((w) => "-".repeat(w + 2)).join("+")}+`;
  const headerLine =
    "| " +
    headers.map((header, i) => pad(header, widths[i])).join(" | ") +
    " |";
  const bodyLines = rowsData.map((row) => {
    const cells = row.base.map((cell, i) => {
      return pad(escapeHtml(cell), widths[i]);
    });
    return "| " + cells.join(" | ") + " |";
  });

  leaderboardOutput.innerHTML = [line, headerLine, line, ...bodyLines, line].join("\n");
  persistLeaderboardState(statKey);
};

const resetLeaderboardSearch = () => {
  if (statSearchInput) {
    statSearchInput.value = "";
  }
  if (statSearchResults) {
    statSearchResults.textContent = "";
  }
};

const persistLeaderboardState = (statKey) => {
  const pitcherRole =
    activeMode === "pitchers" ? getActivePitcherRole() : null;
  leaderboardStateByMode[activeMode] = {
    statKey: statKey || null,
    meta: leaderboardMeta ? leaderboardMeta.textContent : "",
    output: leaderboardOutput ? leaderboardOutput.innerHTML : "",
    year: yearSelect ? yearSelect.value : null,
    pitcherRole,
  };
  persistNow();
};

const clearLeaderboardDisplay = () => {
  if (leaderboardMeta) {
    leaderboardMeta.textContent = "";
  }
  if (leaderboardOutput) {
    leaderboardOutput.textContent = "";
  }
};

const clearLeaderboardSelection = () => {
  activeLeaderboardStat = null;
  const pitcherRole =
    activeMode === "pitchers" ? getActivePitcherRole() : null;
  leaderboardStateByMode[activeMode] = {
    statKey: null,
    meta: "",
    output: "",
    year: yearSelect ? yearSelect.value : null,
    pitcherRole,
  };
  resetLeaderboardSearch();
  clearLeaderboardDisplay();
  persistNow();
};

const restoreLeaderboardForMode = () => {
  const savedState = leaderboardStateByMode[activeMode];
  activeLeaderboardStat = savedState.statKey || null;
  resetLeaderboardSearch();
  updateLeaderboardRoleUI();
  if (!activeLeaderboardStat) {
    clearLeaderboardDisplay();
    return;
  }
  const currentYear = yearSelect ? yearSelect.value : null;
  const hasCachedOutput =
    savedState.output &&
    typeof savedState.output === "string" &&
    savedState.year === currentYear;
  if (leaderboardMeta) {
    leaderboardMeta.textContent = savedState.meta || "";
  }
  if (leaderboardOutput && hasCachedOutput) {
    leaderboardOutput.innerHTML = savedState.output;
  } else {
    const { statsByKey } = getState();
    const statConfig = statsByKey.get(activeLeaderboardStat);
    updateLeaderboardMeta(statConfig);
    void renderLeaderboard(activeLeaderboardStat);
  }
};

searchButton.addEventListener("click", () => runSearch({ showEmptyMessage: true }));
searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    runSearch({ showEmptyMessage: true });
  }
});
searchInput.addEventListener("input", () => {
  runSearch({ showEmptyMessage: false });
});

if (statSearchInput) {
  statSearchInput.addEventListener("input", searchStats);
  statSearchInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      searchStats();
    }
  });
}

if (leaderboardRoleStarters) {
  leaderboardRoleStarters.addEventListener("click", () => {
    setPitcherLeaderboardRole(PITCHER_ROLE_STARTERS);
  });
}

if (leaderboardRoleRelievers) {
  leaderboardRoleRelievers.addEventListener("click", () => {
    setPitcherLeaderboardRole(PITCHER_ROLE_RELIEVERS);
  });
}

clearButton.addEventListener("click", () => {
  searchInput.value = "";
  resultsEl.textContent = "";
  outputPlayer.textContent = "";
  outputCompare.textContent = "";
});

compareButton.addEventListener("click", async () => {
  try {
    const { activeCompareIds } = getState();
    if (
      activeCompareIds.size < 2 ||
      activeCompareIds.size > MAX_COMPARE_PLAYERS
    ) {
      renderMessage("Select 2-5 players to compare.", outputCompare);
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      renderMessage("Select at least one stat.", outputCompare);
      return;
    }

    const dataset = getActiveDataset();
    let rows = [];
    const playerIds = Array.from(activeCompareIds);
    if (isRangeMode()) {
      const rangeRows = await fetchRangeRows(playerIds, statKeys);
      if (!rangeRows.length) {
        renderMessage("No range data available for this selection.", outputCompare);
        return;
      }
      rows = mergeRangeRows(playerIds, rangeRows, dataset);
    } else {
      rows = playerIds
        .map((playerId) => dataset.find((row) => row.player_id === playerId))
        .filter(Boolean);
      if (statKeys.some((key) => isStatcastKeyForMode(key))) {
        const seasonRange = getSeasonFallbackRange(yearSelect.value);
        const rangeRows = await fetchRangeRows(
          playerIds,
          statKeys,
          seasonRange
        );
        if (rangeRows.length) {
          rows = mergeRangeRowsForKeys(playerIds, rangeRows, rows, statKeys);
        }
      }
    }
    console.log(rows);
    renderAsciiTable(rows, statKeys, outputCompare);
  } catch (error) {
    console.log(error);
    renderMessage(error.message || "Range request failed.", outputCompare);
  }
});

viewButton.addEventListener("click", async () => {
  try {
    const { activePlayerId } = getState();
    if (!activePlayerId) {
      renderMessage("Select 1 player to view.", outputPlayer);
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      renderMessage("Select at least one stat.", outputPlayer);
      return;
    }

    const dataset = getActiveDataset();
    let data = dataset.find((row) => row.player_id === activePlayerId);
    if (isRangeMode()) {
      const rangeRows = await fetchRangeRows([activePlayerId], statKeys);
      if (!rangeRows.length) {
        renderMessage("No range data available for this player.", outputPlayer);
        return;
      }
      const merged = mergeRangeRows([activePlayerId], rangeRows, dataset);
      data = merged[0];
    } else if (statKeys.some((key) => isStatcastKeyForMode(key))) {
      const seasonRange = getSeasonFallbackRange(yearSelect.value);
      const rangeRows = await fetchRangeRows(
        [activePlayerId],
        statKeys,
        seasonRange
      );
      if (rangeRows.length) {
        const merged = mergeRangeRowsForKeys(
          [activePlayerId],
          rangeRows,
          [data].filter(Boolean),
          statKeys
        );
        data = merged[0];
      }
    }
    console.log(data);
    if (!data) {
      renderMessage("Player not found in snapshot.", outputPlayer);
      return;
    }
    renderAsciiTable([data], statKeys, outputPlayer);
  } catch (error) {
    console.log(error);
    renderMessage(error.message || "Range request failed.", outputPlayer);
  }
});

if (downloadPlayerButton) {
  downloadPlayerButton.addEventListener("click", () => {
    const { savedPlayers, activePlayerId } = getState();
    const chosen = savedPlayers.find(
      (player) => player.player_id === activePlayerId
    );
    const name = chosen ? slugify(chosen.name) : "player";
    const season = yearSelect.value;
    const stamp = formatDateStamp();
    downloadAsciiAsPng(
      outputPlayer,
      `${activeMode === "pitchers" ? "pitcher" : "player"}_${name}_${season}_${stamp}.png`
    );
  });
}

if (downloadCompareButton) {
  downloadCompareButton.addEventListener("click", () => {
    const { activeCompareIds, savedPlayers } = getState();
    const season = yearSelect.value;
    const stamp = formatDateStamp();
    const names = Array.from(activeCompareIds)
      .map((playerId) => {
        const player = savedPlayers.find((item) => item.player_id === playerId);
        return player ? slugify(player.name) : null;
      })
      .filter(Boolean)
      .slice(0, 3);
    const namePart = names.length ? names.join("_") : "players";
    downloadAsciiAsPng(
      outputCompare,
      `compare_${activeMode}_${namePart}_${season}_${stamp}.png`
    );
  });
}

const loadStatsConfigForMode = async (mode, { render = false } = {}) => {
  const state = stateByMode[mode];
  if (!state) {
    return;
  }
  if (state.configLoaded) {
    if (render && mode === activeMode) {
      renderStatsConfig();
    }
    return;
  }
  try {
    const fileName = CONFIG_FILES[mode] || CONFIG_FILES.hitters;
    const response = await fetch(chrome.runtime.getURL(fileName));
    const data = await response.json();
    state.statsConfig = Array.isArray(data) ? data : [];
    state.configLoaded = true;
    if (render && mode === activeMode) {
      renderStatsConfig();
    }
  } catch (error) {
    console.log(error);
  }
};

const setActiveTab = (tab) => {
  const tabs = [
    { name: "players", button: tabPlayers, panel: panelPlayers },
    { name: "compare", button: tabCompare, panel: panelCompare },
    { name: "teams", button: tabTeams, panel: panelTeams },
    { name: "stats", button: tabStats, panel: panelStats },
    { name: "leaderboard", button: tabLeaderboard, panel: panelLeaderboard },
  ];
  tabs.forEach(({ name, button, panel }) => {
    const isActive = name === tab;
    button.classList.toggle("active", isActive);
    panel.classList.toggle("active", isActive);
  });
};

const resetModeSelections = (state) => {
  state.savedPlayers.length = 0;
  state.activeCompareIds.clear();
  state.activePlayerId = null;
};

const clearOutputs = () => {
  resultsEl.textContent = "";
  outputPlayer.textContent = "";
  outputCompare.textContent = "";
};

const applyMode = async (mode) => {
  if (!stateByMode[mode]) {
    return;
  }
  activeMode = mode;
  await loadStatsConfigForMode(mode, { render: true });
  updateRangeAvailability();
  updatePlayerLimit();
  renderSelected();
  clearOutputs();
  updateLeaderboardRoleUI();
  restoreLeaderboardForMode();
  updateMeta();
  persistNow();
};

yearSelect.addEventListener("change", async () => {
  clearOutputs();
  await loadSnapshot(yearSelect.value);
  await loadPitcherSnapshot(yearSelect.value);
  renderSelected();
  updatePlayerLimit();
  updateMeta();
  restoreLeaderboardForMode();
  persistNow();
});

tabPlayers.addEventListener("click", () => setActiveTab("players"));
tabCompare.addEventListener("click", () => setActiveTab("compare"));
tabTeams.addEventListener("click", () => setActiveTab("teams"));
tabStats.addEventListener("click", () => setActiveTab("stats"));
if (tabLeaderboard) {
  tabLeaderboard.addEventListener("click", () => setActiveTab("leaderboard"));
}

if (browseTeamsButton) {
  browseTeamsButton.addEventListener("click", () => setActiveTab("teams"));
}

if (clearSavedPlayersButton) {
  clearSavedPlayersButton.addEventListener("click", clearSavedPlayers);
}

if (clearSavedPlayersCompareButton) {
  clearSavedPlayersCompareButton.addEventListener("click", clearSavedPlayers);
}

if (rangeEnabledInput) {
  rangeEnabledInput.addEventListener("change", () => {
    updateStatsLimit();
    updateRangeTagsVisibility();
    persistNow();
  });
}

if (rangeStartInput) {
  rangeStartInput.addEventListener("change", schedulePersist);
}

if (rangeEndInput) {
  rangeEndInput.addEventListener("change", schedulePersist);
}

if (modeSelect) {
  modeSelect.addEventListener("change", () => {
    applyMode(modeSelect.value);
  });
}

const init = async () => {
  console.log("[Init] Extension initializing...");
  
  // Step 1: Load persisted state first (silent in non-extension contexts)
  await loadPersistedState();
  
  // Step 2: Clear any stale displays
  renderResults([]);
  renderSelected();
  updatePlayerLimit();
  
  // Step 3: Load snapshots for current year
  await loadSnapshot(yearSelect.value);
  await loadPitcherSnapshot(yearSelect.value);
  
  // Step 4: Apply active mode (this will trigger rendering)
  await applyMode(activeMode);
};

init();
