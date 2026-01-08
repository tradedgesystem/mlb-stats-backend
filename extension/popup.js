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
const panelPlayers = document.getElementById("panel-players");
const panelCompare = document.getElementById("panel-compare");
const panelTeams = document.getElementById("panel-teams");
const panelStats = document.getElementById("panel-stats");

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
let activePlayers = [];
let activePitchers = [];
const activeMetaByMode = { hitters: null, pitchers: null };
let activeTeam = null;
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

const getState = () => stateByMode[activeMode];
const getActiveDataset = () =>
  activeMode === "pitchers" ? activePitchers : activePlayers;
const getActiveMeta = () => activeMetaByMode[activeMode];

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
      return Math.round(value);
    case "float":
      return Number(value.toFixed(1));
    case "rate":
      return Number(value.toFixed(3));
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
  if (activeMode === "pitchers") {
    setRangeWarning("Date ranges are not available for pitchers yet.");
    return;
  }
  const { selectedStatKeys } = getState();
  const removed = [];
  Array.from(selectedStatKeys).forEach((key) => {
    if (!isRangeSupported(key)) {
      selectedStatKeys.delete(key);
      removed.push(key);
      const checkbox = statsEl?.querySelector(
        `input[type=\"checkbox\"][value=\"${key}\"]`
      );
      if (checkbox) {
        checkbox.checked = false;
      }
    }
  });
  if (removed.length) {
    setRangeWarning("Date range supports aggregate stats only. Some were removed.");
  } else {
    setRangeWarning("");
  }
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
  const headers = ["Player", "Team", "Season"];
  statKeys.forEach((key) => {
    const config = statsByKey.get(key);
    headers.push(config?.label || key);
  });

  const bestByKey = new Map();
  if (rows.length > 1) {
    statKeys.forEach((key) => {
      let best = null;
      rows.forEach((row) => {
        const value = row[key];
        if (typeof value === "number" && !Number.isNaN(value)) {
          if (best === null || value > best) {
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
    const base = [row.name, row.team, row.season].map((value) =>
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

  const widths = headers.map((header, index) => {
    const cellWidths = rowsData.map((row) => {
      if (index < 3) {
        return row.base[index].length;
      }
      return row.stats[index - 3].text.length;
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
      const text = pad(escapeHtml(cell.text), widths[index + 3]);
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
    addButton.addEventListener("click", () => addPlayer(player));
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
};

const clearSavedPlayers = () => {
  const { savedPlayers, activeCompareIds } = getState();
  savedPlayers.length = 0;
  activeCompareIds.clear();
  getState().activePlayerId = null;
  renderSelected();
  updatePlayerLimit();
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
  const disableRange = activeMode === "pitchers";
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
  if (disableRange) {
    setRangeWarning("Date ranges are not available for pitchers yet.");
  } else if (!isRangeMode()) {
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
      checkbox.checked = isAvailable
        ? hasSelection
          ? selectedStatKeys.has(item.key)
          : Boolean(item.default)
        : false;
      if (checkbox.checked) {
        selectedStatKeys.add(item.key);
      }
      checkbox.addEventListener("change", () => {
        if (!isAvailable) {
          return;
        }
        if (checkbox.checked) {
          if (isRangeMode() && !item.range_supported) {
            checkbox.checked = false;
            setRangeWarning("Stat not available for date ranges.");
            return;
          }
          if (selectedStatKeys.size >= MAX_STATS) {
            checkbox.checked = false;
            updateStatsLimit();
            return;
          }
          selectedStatKeys.add(item.key);
          if (isRangeMode()) {
            setRangeWarning("");
          }
        } else {
          selectedStatKeys.delete(item.key);
        }
        if (!isRangeMode()) {
          setRangeWarning("");
        }
        updateStatsLimit();
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

searchButton.addEventListener("click", () => runSearch({ showEmptyMessage: true }));
searchInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    runSearch({ showEmptyMessage: true });
  }
});
searchInput.addEventListener("input", () => {
  runSearch({ showEmptyMessage: false });
});

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
    const rows = Array.from(activeCompareIds)
      .map((playerId) => dataset.find((row) => row.player_id === playerId))
      .filter(Boolean);
    console.log(rows);
    renderAsciiTable(rows, statKeys, outputCompare);
  } catch (error) {
    console.log(error);
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
    const data = dataset.find((row) => row.player_id === activePlayerId);
    console.log(data);
    if (!data) {
      renderMessage("Player not found in snapshot.", outputPlayer);
      return;
    }
    renderAsciiTable([data], statKeys, outputPlayer);
  } catch (error) {
    console.log(error);
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
  updateMeta();
};

renderResults([]);
renderSelected();
updatePlayerLimit();
yearSelect.addEventListener("change", async () => {
  Object.values(stateByMode).forEach((state) => resetModeSelections(state));
  renderSelected();
  clearOutputs();
  await loadSnapshot(yearSelect.value);
  await loadPitcherSnapshot(yearSelect.value);
  updatePlayerLimit();
  updateMeta();
});

tabPlayers.addEventListener("click", () => setActiveTab("players"));
tabCompare.addEventListener("click", () => setActiveTab("compare"));
tabTeams.addEventListener("click", () => setActiveTab("teams"));
tabStats.addEventListener("click", () => setActiveTab("stats"));

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
    enforceRangeSelections();
    updateStatsLimit();
    updateRangeTagsVisibility();
  });
}

if (modeSelect) {
  modeSelect.addEventListener("change", () => {
    applyMode(modeSelect.value);
  });
}

loadSnapshot(yearSelect.value);
loadPitcherSnapshot(yearSelect.value);
applyMode(activeMode);
