const button = document.getElementById("load");
const output = document.getElementById("output");

button.addEventListener("click", async () => {
  try {
    const response = await fetch("http://127.0.0.1:8000/players?year=2025");
    const data = await response.json();
    console.log(data);
    output.textContent = JSON.stringify(data.slice(0, 5), null, 2);
  } catch (error) {
    console.log(error);
  }
});
