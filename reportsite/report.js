
async function fetchReport() {
    const table = document.getElementById('report-table');
    try {        
        let response = await fetch('/reports/stats_by_zip.csv');
        if (response.status != 200) {
            throw new Error('report unavailable')
        } else {
            document.getElementById('error').style.display = 'none';
            response = await response.text()
            const rows = response.split('\n');
            for (const row of rows) {
                const cells = row.split(',');
                const newRow = table.insertRow(-1);
                for (const cell of cells) {
                    const newCell = newRow.insertCell(-1);
                    newCell.textContent = cell;
                }
            }
        }
    } catch (e) {
        document.getElementById('error').style.display = 'block';
        document.getElementById('error').innerHTML = e.message;
    }
}

fetchReport();