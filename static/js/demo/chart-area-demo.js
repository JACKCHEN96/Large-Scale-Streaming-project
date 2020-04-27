// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = 'Nunito', '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#858796';

function number_format(number, decimals, dec_point, thousands_sep) {
  // *     example: number_format(1234.56, 2, ',', ' ');
  // *     return: '1 234,56'
  number = (number + '').replace(',', '').replace(' ', '');
  var n = !isFinite(+number) ? 0 : +number,
    prec = !isFinite(+decimals) ? 0 : Math.abs(decimals),
    sep = (typeof thousands_sep === 'undefined') ? ',' : thousands_sep,
    dec = (typeof dec_point === 'undefined') ? '.' : dec_point,
    s = '',
    toFixedFix = function(n, prec) {
      var k = Math.pow(10, prec);
      return '' + Math.round(n * k) / k;
    };
  // Fix for IE parseFloat(0.55).toFixed(0) = 0;
  s = (prec ? toFixedFix(n, prec) : '' + Math.round(n)).split('.');
  if (s[0].length > 3) {
    s[0] = s[0].replace(/\B(?=(?:\d{3})+(?!\d))/g, sep);
  }
  if ((s[1] || '').length < prec) {
    s[1] = s[1] || '';
    s[1] += new Array(prec - s[1].length + 1).join('0');
  }
  return s.join(dec);
}


var httpRequest = new XMLHttpRequest();
var url='http://18.216.222.18:8099/PROTOTYPE_V2/getall';
httpRequest.open('GET', url, true);
httpRequest.setRequestHeader("Content-Type","application/x-www-form-urlencoded");
httpRequest.send(null);
httpRequest.onreadystatechange = function () {
 if (httpRequest.readyState == 4 && httpRequest.status == 200) {
     var res = JSON.parse(httpRequest.responseText);

     //console.log(res["result"][0]['time'].toString());
     // Area Chart Example
      var ctx = document.getElementById("myAreaChart");
      var myLineChart = new Chart(ctx, {
        type: 'line',
        data: {
          //labels: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct"],
          labels: [
            res["result"][0]['time'].toString(),
            res["result"][1]['time'].toString(),
            res["result"][2]['time'].toString(),
            res["result"][3]['time'].toString(),
            res["result"][4]['time'].toString(),
            res["result"][5]['time'].toString(),
            res["result"][6]['time'].toString(),
            res["result"][7]['time'].toString(),
            res["result"][8]['time'].toString(),
            res["result"][9]['time'].toString()],
          datasets: [{
            label: "Earnings",
            lineTension: 0.3,
            backgroundColor: "rgba(78, 115, 223, 0.05)",
            borderColor: "rgba(78, 115, 223, 1)",
            pointRadius: 3,
            pointBackgroundColor: "rgba(78, 115, 223, 1)",
            pointBorderColor: "rgba(78, 115, 223, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor: "rgba(78, 115, 223, 1)",
            pointHoverBorderColor: "rgba(78, 115, 223, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            data: [
            res["result"][0]['data_1'],
            res["result"][1]['data_1'],
            res["result"][2]['data_1'],
            res["result"][3]['data_1'],
            res["result"][4]['data_1'],
            res["result"][5]['data_1'],
            res["result"][6]['data_1'],
            res["result"][7]['data_1'],
            res["result"][8]['data_1'],
            res["result"][9]['data_1']
            ]
          },
          {
            label: "Earnings",
            lineTension: 0.3,
            backgroundColor: "rgba(78, 115, 223, 0.05)",
            borderColor: "rgba(78, 225, 223, 1)",
            pointRadius: 3,
            pointBackgroundColor: "rgba(78, 225, 223, 1)",
            pointBorderColor: "rgba(78, 225, 223, 1)",
            pointHoverRadius: 3,
            pointHoverBackgroundColor: "rgba(78, 115, 223, 1)",
            pointHoverBorderColor: "rgba(78, 115, 223, 1)",
            pointHitRadius: 10,
            pointBorderWidth: 2,
            data: [
            res["result"][0]['data_2'],
            res["result"][1]['data_2'],
            res["result"][2]['data_2'],
            res["result"][3]['data_2'],
            res["result"][4]['data_2'],
            res["result"][5]['data_2'],
            res["result"][6]['data_2'],
            res["result"][7]['data_2'],
            res["result"][8]['data_2'],
            res["result"][9]['data_2']
            ]
          }],
        },
        options: {
          maintainAspectRatio: false,
          layout: {
            padding: {
              left: 10,
              right: 25,
              top: 25,
              bottom: 0
            }
          },
          scales: {
            xAxes: [{
              time: {
                unit: 'date'
              },
              gridLines: {
                display: false,
                drawBorder: false
              },
              ticks: {
                maxTicksLimit: 7
              }
            }],
            yAxes: [{
              ticks: {
                maxTicksLimit: 5,
                padding: 10,
                // Include a dollar sign in the ticks
                callback: function(value, index, values) {
                  return number_format(value);
                }
              },
              gridLines: {
                color: "rgb(234, 236, 244)",
                zeroLineColor: "rgb(234, 236, 244)",
                drawBorder: false,
                borderDash: [2],
                zeroLineBorderDash: [2]
              }
            }],
          },
          legend: {
            display: false
          },
          tooltips: {
            backgroundColor: "rgb(255,255,255)",
            bodyFontColor: "#858796",
            titleMarginBottom: 10,
            titleFontColor: '#6e707e',
            titleFontSize: 14,
            borderColor: '#dddfeb',
            borderWidth: 1,
            xPadding: 15,
            yPadding: 15,
            displayColors: false,
            intersect: false,
            mode: 'index',
            caretPadding: 10,
            callbacks: {
              label: function(tooltipItem, chart) {
                var datasetLabel = chart.datasets[tooltipItem.datasetIndex].label || '';
                return datasetLabel + ':' + chart.datasets[tooltipItem.datasetIndex].data[tooltipItem.index];
              }
            }
          }
        }
      });
     }
  };


