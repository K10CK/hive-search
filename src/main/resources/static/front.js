async function getSearch() {
    var searchQuery = document.getElementById("searchInput").value;
    var selectedHeader = document.getElementById("headerDropdown").value;

    if (!selectedHeader) {
        alert("Please select a header to search within.");
        return;
    }

    let url = 'http://localhost:8080/hive/default/dataset2/search/' + selectedHeader + '/' + searchQuery;
    let response = await fetch(url);
    var data = await response.json();
    jsonArray = [];

    for (i = 0; i < data.length; i++) {
        jsonArray.push(data[i]);
    }
    if (jsonArray.length == 0) {
        alert("No results from search. Please try a different query");
        return;
    }

    let container = document.getElementById("container");
    container.innerHTML = ''; // Clear container first

    let rowCount = data.length;
    let rowCountElement = document.createElement("p");
    rowCountElement.innerText = "Number of results: " + rowCount;
    container.appendChild(rowCountElement);

    let table = document.createElement("table");
    table.classList.add("table"); // Add Bootstrap table styling

    let cols = Object.keys(data[0]);

    let header = document.createElement("thead");
    let headerRow = document.createElement("tr");

    cols.forEach((item) => {
        let cell = document.createElement("th"); // Use <th> for table header cells
        cell.innerText = item;
        headerRow.appendChild(cell);
    });
    header.appendChild(headerRow);
    table.appendChild(header);

    let tbody = document.createElement("tbody");

    data.forEach((item) => {
        let row = document.createElement("tr"); // Use <tr> for table rows

        let vals = Object.values(item);

        vals.forEach((elem) => {
            let cell = document.createElement("td"); // Use <td> for table cells
            cell.innerText = elem;
            row.appendChild(cell);
        });
        tbody.appendChild(row);
    });

    table.appendChild(tbody);
    container.appendChild(table);
}



async function excel() {
    var searchQuery = document.getElementById("searchInput").value;
    var selectedHeader = document.getElementById("headerDropdown").value;

    if (!selectedHeader) {
        alert("Please select a header to search within.");
        return;
    }

    let url = 'http://localhost:8080/hive/default/dataset2/search/' + selectedHeader + '/' + searchQuery;
    let response = await fetch(url);
    var data = await response.json();
    jsonArray = [];

    for (i = 0; i < data.length; i++) {
        jsonArray.push(data[i]);
    }
    if (jsonArray.length == 0) {
        alert("No results from search. Please try a different query");
        return;
    }
    var excel = 'sep=,' + '\r\n\n';
    var value = "";
    for (var index in jsonArray[0]) {
        value += index + ',';
    }
    value = value.slice(0, -1);
    excel += value + '\r\n';

    // Double for loop to extract specific values
    for (var i = 0; i < jsonArray.length; i++) {
        var value = "";
        for (var index in jsonArray[i]) {
            value += '"' + jsonArray[i][index] + '",';
        }
        value.slice(0, value.length - 1);
        excel += value + '\r\n';
    }

    // Create element to link file to
    var link = document.createElement("a");
    // Convert excel into excel file and link to element
    link.href = 'data:text/csv;charset=utf-8,' + escape(excel);

    // Hide element and use to download file automatically
    link.style = "visibility:hidden";
    link.download = searchQuery + ".csv";

    // Remove element after downloading the file
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function uploadFile() {
    var fileInput = document.getElementById('fileInput');
    var file = fileInput.files[0];

    if (!file) {
        alert('Please select a file to upload.');
        return;
    }

    var formData = new FormData();
    formData.append('file', file);

    fetch('/default/dataset2/upload', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (response.ok) {
            return response.text();
        } else {
            return response.text().then(errorMessage => Promise.reject(new Error(errorMessage)));
        }
    })
    .then(data => {
        fetchCleanedHeaders(); // Fetch and populate cleaned headers
    })
    .catch(error => {
        console.error(error);
        alert('File upload failed: ' + error.message);
    });
}


function fetchCleanedHeaders() {
    fetch('/default/dataset2/headers')
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                return response.text().then(errorMessage => Promise.reject(new Error(errorMessage)));
            }
        })
        .then(headers => {
            var dropdown = document.getElementById('headerDropdown');

            // Clear existing options
            dropdown.innerHTML = '';

            // Create and add options based on cleaned headers
            headers.forEach(header => {
                var option = document.createElement('option');
                option.value = header;
                option.text = header;
                dropdown.appendChild(option);
            });
        })
        .catch(error => {
            console.error('Error fetching headers:', error);
            alert('Error fetching headers: ' + error.message);
        });
}



//function showMessage(message, type) {
//    var messageDiv = document.getElementById('message');
//    messageDiv.innerHTML = '';
//    messageDiv.innerHTML = message;
//    messageDiv.className = type;
//}