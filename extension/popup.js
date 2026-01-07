const button = document.getElementById("load");
const output = document.getElementById("output");
const yearSelect = document.getElementById("year");

button.addEventListener("click", async () => {
  try {
    const year = yearSelect.value;
    const response = await fetch(
      `http://127.0.0.1:8000/players?year=${encodeURIComponent(year)}`
    );
    const data = await response.json();
    console.log(data);
    const rows = Array.isArray(data) ? data : [];
    output.textContent = JSON.stringify(rows.slice(0, 5), null, 2);
  } catch (error) {
    console.log(error);
  }
});
