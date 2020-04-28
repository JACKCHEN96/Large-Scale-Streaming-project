// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = 'Nunito', '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#858796';

// Pie Chart Example


function PieChartType(ctx, label, data, color) {
    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: label,
            datasets: [{
                data: data,
                backgroundColor: color,
                hoverBackgroundColor: ['#2e59d9', '#17a673', '#2c9faf'],
                hoverBorderColor: "rgba(234, 236, 244, 1)",
            }],
        },
        options: {
            maintainAspectRatio: false,
            tooltips: {
                backgroundColor: "rgb(255,255,255)",
                bodyFontColor: "#858796",
                borderColor: '#dddfeb',
                borderWidth: 1,
                xPadding: 15,
                yPadding: 15,
                displayColors: false,
                caretPadding: 10,
            },
            legend: {
                display: true,
                position: 'left',
                labels: {
                    fontColor: "grey",
                    boxWidth: 10,
                    padding: 10,
                    usePointStyle: true,
                }
            },

            cutoutPercentage: 80,
        },

    });
}

function getColors(length) {
    let pallet = ["#0074D9", "#FF4136", "#2ECC40", "#FF851B", "#7FDBFF", "#B10DC9", "#FFDC00", "#001f3f", "#39CCCC", "#01FF70", "#85144b", "#F012BE", "#3D9970", "#111111", "#AAAAAA"];
    let colors = [];

    for (let i = 0; i < length; i++) {
        colors.push(pallet[i % pallet.length]);
    }

    return colors;
}

function updateChart(myChart, color) {
    var xmlhttp;
    if (window.XMLHttpRequest) {
        // IE7+, Firefox, Chrome, Opera, Safari
        xmlhttp = new XMLHttpRequest();
    } else {
        // IE6, IE5
        xmlhttp = new ActiveXObject("Microsoft.XMLHTTP");
    }
    xmlhttp.onreadystatechange = function () {
        if (xmlhttp.readyState == 4 && xmlhttp.status == 200) {

            var json = xmlhttp.responseText;
            var jstr = JSON.stringify(json);
            var option = JSON.parse(JSON.parse(jstr));
            // console.log(option);
            myChart.data = {
                labels: option.label,
                datasets: [{
                    data: option.data,
                    backgroundColor: color,
                    hoverBackgroundColor: ['#2e59d9', '#17a673', '#2c9faf'],
                    hoverBorderColor: "rgba(234, 236, 244, 1)",
                }],
            };
            myChart.update(0);
        }
    };
    xmlhttp.open("GET", "/data/template1", true);
    xmlhttp.send();
}