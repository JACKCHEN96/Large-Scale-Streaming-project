// Call the dataTables jQuery plugin

function updateTablePlatform(tbl) {

    var xmlhttp;
    if (window.XMLHttpRequest) {
        // IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    } else {
        // IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }
    xmlhttp.onreadystatechange = function() {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {

            var json = xmlhttp.responseText;
            var jstr = JSON.stringify(json);
            var dataset = JSON.parse(JSON.parse(jstr));
            // console.log(dataset);
            tbl.DataTable(dataset);
        }
    };
    xmlhttp.open("GET", "/data/table_platform", true);
    xmlhttp.send();

}