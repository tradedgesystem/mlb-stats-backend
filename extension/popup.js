const yearSelect = document.getElementById("year");
const searchInput = document.getElementById("search");
const searchButton = document.getElementById("search-btn");
const clearButton = document.getElementById("clear-btn");
const resultsEl = document.getElementById("results");
const selectedEl = document.getElementById("selected");
const viewButton = document.getElementById("view-btn");
const compareButton = document.getElementById("compare-btn");
const statsEl = document.getElementById("stats");
const output = document.getElementById("output");
const metaEl = document.getElementById("snapshot-meta");
const warningEl = document.getElementById("snapshot-warning");
const statsLimitEl = document.getElementById("stats-limit");
const tabPlayers = document.getElementById("tab-players");
const tabStats = document.getElementById("tab-stats");
const panelPlayers = document.getElementById("panel-players");
const panelStats = document.getElementById("panel-stats");

const selectedPlayers = [];
let statsConfig = [];
const statsByKey = new Map();
const selectedStatKeys = new Set();
const snapshotsByYear = new Map();
let activePlayers = [];
let activeMeta = null;
const SNAPSHOT_BASE_URL =
  "https://cdn.jsdelivr.net/gh/tradedgesystem/mlb-stats-backend@main/extension/snapshots";
const MAX_STATS = 10;

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
  if (!statsLimitEl) {
    return;
  }
  if (selectedStatKeys.size >= MAX_STATS) {
    statsLimitEl.textContent = `Max ${MAX_STATS} stats selected.`;
  } else {
    statsLimitEl.textContent = "";
  }
};

const renderMessage = (message) => {
  output.textContent = message;
};

const renderTable = (rows, statKeys) => {
  output.textContent = "";
  if (!rows.length) {
    renderMessage("No data to display.");
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
  output.appendChild(table);
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

const renderSelected = () => {
  if (!selectedPlayers.length) {
    selectedEl.textContent = "None selected.";
    return;
  }

  selectedEl.innerHTML = "";
  selectedPlayers.forEach((player) => {
    const row = document.createElement("div");
    row.textContent = `${player.name} (${player.team})`;
    const removeButton = document.createElement("button");
    removeButton.textContent = "Remove";
    removeButton.addEventListener("click", () => removePlayer(player.player_id));
    row.appendChild(removeButton);
    selectedEl.appendChild(row);
  });
};

const addPlayer = (player) => {
  if (selectedPlayers.find((item) => item.player_id === player.player_id)) {
    return;
  }
  if (selectedPlayers.length >= 5) {
    return;
  }
  selectedPlayers.push(player);
  renderSelected();
};

const removePlayer = (playerId) => {
  const index = selectedPlayers.findIndex((item) => item.player_id === playerId);
  if (index === -1) {
    return;
  }
  selectedPlayers.splice(index, 1);
  renderSelected();
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
  output.textContent = "";
});

compareButton.addEventListener("click", async () => {
  try {
    if (selectedPlayers.length < 2 || selectedPlayers.length > 5) {
      renderMessage("Select 2-5 players to compare.");
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      renderMessage("Select at least one stat.");
      return;
    }

    const rows = selectedPlayers
      .map((player) =>
        activePlayers.find((row) => row.player_id === player.player_id)
      )
      .filter(Boolean);
    console.log(rows);
    renderTable(rows, statKeys);
  } catch (error) {
    console.log(error);
  }
});

viewButton.addEventListener("click", async () => {
  try {
    if (selectedPlayers.length !== 1) {
      renderMessage("Select 1 player to view.");
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      renderMessage("Select at least one stat.");
      return;
    }

    const playerId = selectedPlayers[0].player_id;
    const data = activePlayers.find((row) => row.player_id === playerId);
    console.log(data);
    if (!data) {
      renderMessage("Player not found in snapshot.");
      return;
    }
    renderTable([data], statKeys);
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
  if (tab === "stats") {
    tabStats.classList.add("active");
    tabPlayers.classList.remove("active");
    panelStats.classList.add("active");
    panelPlayers.classList.remove("active");
    return;
  }
  tabPlayers.classList.add("active");
  tabStats.classList.remove("active");
  panelPlayers.classList.add("active");
  panelStats.classList.remove("active");
};

renderResults([]);
renderSelected();
yearSelect.addEventListener("change", async () => {
  selectedPlayers.length = 0;
  renderSelected();
  resultsEl.textContent = "";
  output.textContent = "";
  await loadSnapshot(yearSelect.value);
});

tabPlayers.addEventListener("click", () => setActiveTab("players"));
tabStats.addEventListener("click", () => setActiveTab("stats"));

loadStatsConfig();
loadSnapshot(yearSelect.value);
