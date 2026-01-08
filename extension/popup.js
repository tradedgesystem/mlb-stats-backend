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

const selectedPlayers = [];
let statsConfig = [];
const selectedStatKeys = new Set();

const getSelectedKeys = () => {
  if (!statsConfig.length) {
    return [];
  }
  return Array.from(selectedStatKeys);
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

const renderStatsConfig = (config) => {
  statsEl.textContent = "";
  const groupOrder = [];
  const groups = {};

  config.forEach((item) => {
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
    const year = yearSelect.value;
    const query = searchInput.value.trim();
    if (!query) {
      resultsEl.textContent = "Enter a search term.";
      return;
    }

    const response = await fetch(
      `http://127.0.0.1:8000/search?year=${encodeURIComponent(
        year
      )}&q=${encodeURIComponent(query)}`
    );
    const data = await response.json();
    console.log(data);
    renderResults(Array.isArray(data) ? data : []);
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
    const year = yearSelect.value;
    if (selectedPlayers.length < 2 || selectedPlayers.length > 5) {
      output.textContent = "Select 2-5 players to compare.";
      return;
    }
    const statKeys = getSelectedKeys();
    if (!statKeys.length) {
      output.textContent = "Select at least one stat.";
      return;
    }

    const ids = selectedPlayers.map((player) => player.player_id).join(",");
    const response = await fetch(
      `http://127.0.0.1:8000/compare?year=${encodeURIComponent(
        year
      )}&player_ids=${encodeURIComponent(ids)}`
    );
    const data = await response.json();
    console.log(data);
    const rows = Array.isArray(data) ? data : [];
    const filtered = rows.map((row) => {
      const result = {
        player_id: row.player_id,
        name: row.name,
        team: row.team,
        season: row.season,
      };
      statKeys.forEach((key) => {
        result[key] = row[key];
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
    const year = yearSelect.value;
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
    const response = await fetch(
      `http://127.0.0.1:8000/player?year=${encodeURIComponent(
        year
      )}&player_id=${encodeURIComponent(playerId)}`
    );
    const data = await response.json();
    console.log(data);
    const filtered = {
      player_id: data.player_id,
      name: data.name,
      team: data.team,
      season: data.season,
    };
    statKeys.forEach((key) => {
      filtered[key] = data[key];
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
loadStatsConfig();
