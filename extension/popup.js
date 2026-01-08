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

const selectedPlayers = [];
let statsConfig = [];
const statsByKey = new Map();
const selectedStatKeys = new Set();
const snapshotsByYear = new Map();
let activePlayers = [];
let activeMeta = null;
const SNAPSHOT_BASE_URL =
  "https://cdn.jsdelivr.net/gh/tradedgesystem/mlb-stats-backend@main/extension/snapshots";

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
    return;
  }
  const timestamp = activeMeta.generated_at;
  const date = timestamp ? new Date(timestamp) : null;
  const readable = date && !Number.isNaN(date.getTime())
    ? date.toLocaleString()
    : "unavailable";
  metaEl.textContent = `Data updated: ${readable}`;
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
          selectedStatKeys.add(item.key);
        } else {
          selectedStatKeys.delete(item.key);
        }
      });

      const text = document.createElement("span");
      text.textContent = item.label;

      row.appendChild(checkbox);
      row.appendChild(text);
      groupEl.appendChild(row);
    });

    statsEl.appendChild(groupEl);
  });
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
      output.textContent = "Select 2-5 players to compare.";
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      output.textContent = "Select at least one stat.";
      return;
    }

    const rows = selectedPlayers
      .map((player) =>
        activePlayers.find((row) => row.player_id === player.player_id)
      )
      .filter(Boolean);
    console.log(rows);
    const filtered = rows.map((row) => {
      const result = {
        player_id: row.player_id,
        name: row.name,
        team: row.team,
        season: row.season,
      };
      statKeys.forEach((key) => {
        const config = statsByKey.get(key);
        const value = row[key];
        result[key] = formatValue(value ?? null, config?.format);
      });
      return result;
    });
    output.textContent = JSON.stringify(filtered, null, 2);
  } catch (error) {
    console.log(error);
  }
});

viewButton.addEventListener("click", async () => {
  try {
    if (selectedPlayers.length !== 1) {
      output.textContent = "Select 1 player to view.";
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      output.textContent = "Select at least one stat.";
      return;
    }

    const playerId = selectedPlayers[0].player_id;
    const data = activePlayers.find((row) => row.player_id === playerId);
    console.log(data);
    if (!data) {
      output.textContent = "Player not found in snapshot.";
      return;
    }
    const filtered = {
      player_id: data.player_id,
      name: data.name,
      team: data.team,
      season: data.season,
    };
    statKeys.forEach((key) => {
      const config = statsByKey.get(key);
      const value = data[key];
      filtered[key] = formatValue(value ?? null, config?.format);
    });
    output.textContent = JSON.stringify(filtered, null, 2);
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

renderResults([]);
renderSelected();
yearSelect.addEventListener("change", async () => {
  selectedPlayers.length = 0;
  renderSelected();
  resultsEl.textContent = "";
  output.textContent = "";
  await loadSnapshot(yearSelect.value);
});

loadStatsConfig();
loadSnapshot(yearSelect.value);
