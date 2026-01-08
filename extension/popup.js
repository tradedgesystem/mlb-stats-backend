const yearSelect = document.getElementById("year");
const searchInput = document.getElementById("search");
const searchButton = document.getElementById("search-btn");
const clearButton = document.getElementById("clear-btn");
const resultsEl = document.getElementById("results");
const savedPlayersEl = document.getElementById("saved-players");
const savedPlayersCompareEl = document.getElementById("saved-players-compare");
const selectedStatsPlayersEl = document.getElementById("selected-stats-players");
const selectedStatsCompareEl = document.getElementById("selected-stats-compare");
const viewButton = document.getElementById("view-btn");
const compareButton = document.getElementById("compare-run-btn");
const statsEl = document.getElementById("stats");
const outputPlayer = document.getElementById("output-player");
const outputCompare = document.getElementById("output-compare");
const metaEl = document.getElementById("snapshot-meta");
const warningEl = document.getElementById("snapshot-warning");
const statsLimitEl = document.getElementById("stats-limit");
const statsCountEl = document.getElementById("stats-count-players");
const playersLimitEl = document.getElementById("players-limit");
const compareLimitEl = document.getElementById("compare-limit");
const chosenPlayerEl = document.getElementById("chosen-player");
const tabPlayers = document.getElementById("tab-players");
const tabCompare = document.getElementById("tab-compare");
const tabStats = document.getElementById("tab-stats");
const panelPlayers = document.getElementById("panel-players");
const panelCompare = document.getElementById("panel-compare");
const panelStats = document.getElementById("panel-stats");

const savedPlayers = [];
let activePlayerId = null;
const activeCompareIds = new Set();
let statsConfig = [];
const statsByKey = new Map();
const selectedStatKeys = new Set();
const snapshotsByYear = new Map();
let activePlayers = [];
let activeMeta = null;
const SNAPSHOT_BASE_URL =
  "https://cdn.jsdelivr.net/gh/tradedgesystem/mlb-stats-backend@main/extension/snapshots";
const MAX_STATS = 10;
const MAX_SAVED_PLAYERS = 10;
const MAX_COMPARE_PLAYERS = 5;

const getSelectedKeys = () => {
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

const updateMeta = () => {
  if (!metaEl) {
    return;
  }
  if (!activeMeta) {
    metaEl.textContent = "Data updated: unavailable";
    if (warningEl) {
      warningEl.textContent = "";
    }
    return;
  }
  const timestamp = activeMeta.generated_at;
  const date = timestamp ? new Date(timestamp) : null;
  const readable = date && !Number.isNaN(date.getTime())
    ? date.toLocaleString()
    : "unavailable";
  metaEl.textContent = `Data updated: ${readable}`;

  if (warningEl) {
    if (!date || Number.isNaN(date.getTime())) {
      warningEl.textContent = "Snapshot freshness unknown.";
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

const updatePlayerLimit = () => {
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

const renderTable = (rows, statKeys, target) => {
  if (!target) {
    return;
  }
  target.textContent = "";
  if (!rows.length) {
    renderMessage("No data to display.", target);
    return;
  }

  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  const baseHeaders = ["Name", "Team", "Season"];
  baseHeaders.forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });

  statKeys.forEach((key) => {
    const config = statsByKey.get(key);
    const th = document.createElement("th");
    th.textContent = config?.label || key;
    headRow.appendChild(th);
  });

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    [row.name, row.team, row.season].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value ?? "-";
      tr.appendChild(td);
    });

    statKeys.forEach((key) => {
      const config = statsByKey.get(key);
      const value = formatValue(row[key], config?.format);
      const td = document.createElement("td");
      td.textContent = value ?? "-";
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  target.appendChild(table);
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

const renderSelectedList = (container) => {
  if (!container) {
    return;
  }
  container.innerHTML = "";
  if (!savedPlayers.length) {
    container.textContent = "No players saved.";
    return;
  }
  const isPlayersTab = container === savedPlayersEl;
  savedPlayers.forEach((player) => {
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
        activePlayerId = player.player_id;
      } else {
        if (activeCompareIds.has(player.player_id)) {
          activeCompareIds.delete(player.player_id);
        } else {
          if (activeCompareIds.size >= MAX_COMPARE_PLAYERS) {
            return;
          }
          activeCompareIds.add(player.player_id);
        }
      }
      updatePlayerLimit();
      renderSelected();
    });
    container.appendChild(button);
  });
};

const renderSelected = () => {
  renderSelectedList(savedPlayersEl);
  renderSelectedList(savedPlayersCompareEl);
};

const addPlayer = (player) => {
  if (savedPlayers.find((item) => item.player_id === player.player_id)) {
    return;
  }
  if (savedPlayers.length >= MAX_SAVED_PLAYERS) {
    return;
  }
  savedPlayers.push(player);
  if (!activePlayerId) {
    activePlayerId = player.player_id;
  }
  renderSelected();
  updatePlayerLimit();
};

const loadSnapshot = async (year) => {
  if (snapshotsByYear.has(year)) {
    const cached = snapshotsByYear.get(year);
    activePlayers = cached.players;
    activeMeta = cached.meta;
    updateMeta();
    return;
  }

  try {
    const response = await fetch(`${SNAPSHOT_BASE_URL}/players_${year}.json`);
    const data = await response.json();
    const players = Array.isArray(data) ? data : data.players || [];
    const meta = Array.isArray(data) ? null : data.meta || null;
    snapshotsByYear.set(year, { players, meta });
    activePlayers = players;
    activeMeta = meta;
  } catch (error) {
    console.log(error);
    activePlayers = [];
    activeMeta = null;
  }
  updateMeta();
};

const renderStatsConfig = (config) => {
  statsEl.textContent = "";
  const groupOrder = [];
  const groups = {};

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

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = item.key;
      checkbox.checked = Boolean(item.default);
      if (checkbox.checked) {
        selectedStatKeys.add(item.key);
      }
      checkbox.addEventListener("change", () => {
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
      row.appendChild(checkbox);
      row.appendChild(textWrap);
      groupEl.appendChild(row);
    });

    statsEl.appendChild(groupEl);
  });

  updateStatsLimit();
};

searchButton.addEventListener("click", async () => {
  try {
    const query = searchInput.value.trim();
    if (!query) {
      resultsEl.textContent = "Enter a search term.";
      return;
    }

    const matches = activePlayers
      .filter((player) =>
        player.name.toLowerCase().includes(query.toLowerCase())
      )
      .slice(0, 50)
      .map((player) => ({
        player_id: player.player_id,
        name: player.name,
        team: player.team,
      }));
    console.log(matches);
    renderResults(matches);
  } catch (error) {
    console.log(error);
  }
});

clearButton.addEventListener("click", () => {
  searchInput.value = "";
  resultsEl.textContent = "";
  outputPlayer.textContent = "";
  outputCompare.textContent = "";
});

compareButton.addEventListener("click", async () => {
  try {
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

    const rows = Array.from(activeCompareIds)
      .map((playerId) =>
        activePlayers.find((row) => row.player_id === playerId)
      )
      .filter(Boolean);
    console.log(rows);
    renderTable(rows, statKeys, outputCompare);
  } catch (error) {
    console.log(error);
  }
});

viewButton.addEventListener("click", async () => {
  try {
    if (!activePlayerId) {
      renderMessage("Select 1 player to view.", outputPlayer);
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      renderMessage("Select at least one stat.", outputPlayer);
      return;
    }

    const data = activePlayers.find((row) => row.player_id === activePlayerId);
    console.log(data);
    if (!data) {
      renderMessage("Player not found in snapshot.", outputPlayer);
      return;
    }
    renderTable([data], statKeys, outputPlayer);
  } catch (error) {
    console.log(error);
  }
});

const loadStatsConfig = async () => {
  try {
    const response = await fetch(chrome.runtime.getURL("stats_config.json"));
    const data = await response.json();
    statsConfig = Array.isArray(data) ? data : [];
    renderStatsConfig(statsConfig);
  } catch (error) {
    console.log(error);
  }
};

const setActiveTab = (tab) => {
  const tabs = [
    { name: "players", button: tabPlayers, panel: panelPlayers },
    { name: "compare", button: tabCompare, panel: panelCompare },
    { name: "stats", button: tabStats, panel: panelStats },
  ];
  tabs.forEach(({ name, button, panel }) => {
    const isActive = name === tab;
    button.classList.toggle("active", isActive);
    panel.classList.toggle("active", isActive);
  });
};

renderResults([]);
renderSelected();
yearSelect.addEventListener("change", async () => {
  savedPlayers.length = 0;
  activeCompareIds.clear();
  activePlayerId = null;
  renderSelected();
  resultsEl.textContent = "";
  outputPlayer.textContent = "";
  outputCompare.textContent = "";
  await loadSnapshot(yearSelect.value);
  updatePlayerLimit();
});

tabPlayers.addEventListener("click", () => setActiveTab("players"));
tabCompare.addEventListener("click", () => setActiveTab("compare"));
tabStats.addEventListener("click", () => setActiveTab("stats"));

loadStatsConfig();
loadSnapshot(yearSelect.value);
