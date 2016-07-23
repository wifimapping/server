// Create additional Control placeholders for Leaflet
function addControlPlaceholders(map) {
    var corners = map._controlCorners,
        l = 'leaflet-',
        container = map._controlContainer;

    function createCorner(vSide, hSide) {
        var className = l + vSide + ' ' + l + hSide;

        corners[vSide + hSide] = L.DomUtil.create('div', className, container);
    }

    createCorner('verticalcenter', 'left');
    createCorner('verticalcenter', 'right');
}


/*function median(values) {
    values.sort((a, b) => a - b);
    return (values[(values.length - 1) >> 1] + values[values.length >> 1]) / 2
}

function max(values) {
    return values.sort((a, b) => a - b)[values.length - 1];
}

function mean(values) {
    return (values.reduce((a, b) => a + b, 0) / values.length);
}

function range(values) {
    values.sort((a, b) => a - b);
    return [values[10], values[values.length-10]];
}

var AGGREGATION = {
    median: median,
    max: max,
    mean: mean
};*/


angular
.module('Heatmap', [])
.controller('Ctrl', function($scope) {
    $scope.params = {
        aggregation: 'median',
        ssid: 'All Wi-Fi'
        //ssid: 'nyu'
    };

    //var resultCache = {};

    var baseLayer = L.tileLayer(
      'http://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', {
        attribution: 'Map data &copy; CartoDB',
        maxZoom: 17
      }
    );

    var heatmapLayer = L.tileLayer(
          'http://wifindproject.com/wifipulling/tile/{z}/{x}/{y}/?ssid={ssid}&agg_function={agg_function}', {
            ssid: 'All Wi-Fi',
            //ssid: 'nyu',
            agg_function: 'median',
            maxZoom: 18,
            opacity: .5
          }
        );
    
    var greyLayer1 = L.tileLayer(
          'http://wifindproject.com/wifipulling/greyTile/{z}/{x}/{y}', {
            maxZoom: 18,
            opacity: .4
          }
        );

    var map = new L.Map('map', {
      center: new L.LatLng(40.7155134, -73.9964609),
      zoom: 13,
      maxZoom: 17,
      minZoom: 12,
      scrollWheelZoom: false, 
      layers: [baseLayer, greyLayer1, heatmapLayer]
    });

    addControlPlaceholders(map);
    map.zoomControl.setPosition('verticalcenterleft');
    
    //map.addLayer(heatmapLayer);
    //map.addLayer(greyLayer1);

    map.on('zoomstart', function() {
        window.stop();
    });

    $scope.execute = function() {
        if (heatmapLayer) {
            map.removeLayer(heatmapLayer);
        } else {
            //map.on('zoomend', function() {
            //    $scope.execute();
            //});
        }
        
        /*greyLayer = L.tileLayer(
          'http://capstone.cloudapp.net/wifipulling/greyTile/{z}/{x}/{y}', {
            maxZoom: 18,
            opacity: .3
          }
        );

        map.addLayer(greyLayer);*/

        heatmapLayer = L.tileLayer(
          'http://wifindproject.com/wifipulling/tile/{z}/{x}/{y}/?ssid={ssid}&agg_function={agg_function}', {
            ssid: $scope.params.ssid,
            agg_function: $scope.params.aggregation,
            maxZoom: 18,
            opacity: .5
          }
        );

        map.addLayer(heatmapLayer);

        /*map.on('zoomend', function() {
            $scope.execute();
        });*/

    };
});
