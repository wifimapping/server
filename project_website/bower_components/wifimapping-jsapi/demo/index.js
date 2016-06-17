angular
.module('DemoApp', ['WifiMapping'])
.controller('DemoCtrl', function($scope, wifiMappingAPI) {
    $scope.params = {};
    for (var i in wifiMappingAPI.queryParams) {
        $scope.params[wifiMappingAPI.queryParams[i]] = null;
    }
    delete $scope.params['columns'];
    $scope.params['page_size'] = 5;
    $scope.params['page'] = 0;
    $scope.params['startdate'] = '5/10/2016';
    $scope.params['ssid'] = 'nyu';

    $scope.columns = {};
    for (var i in wifiMappingAPI.queryColumns) {
        $scope.columns[wifiMappingAPI.queryColumns[i]] = false;
    }

    $scope.columns['ssid'] = true;
    $scope.columns['level'] = true;
    $scope.columns['time'] = true;

    $scope.execute = function() {
        $scope.text = 'Pending...';

        var columns = [];
        for (var col in $scope.columns) {
            if ($scope.columns[col]) {
                columns.push(col);
            }
        }
        var params = {columns: columns};
        for (param in $scope.params) {
            if ($scope.params[param]) {
                params[param] = $scope.params[param];
            }
        }
        wifiMappingAPI.query(params).then(function(res) {
            $scope.text = res;
        });
    }
})
.controller('DemoCtrl2', function($scope, wifiMappingAPI) {
    $scope.params = {};
    for (var i in wifiMappingAPI.queryParams) {
        $scope.params[wifiMappingAPI.queryParams[i]] = null;
    }
    delete $scope.params['columns'];
    $scope.params['page_size'] = 5;
    $scope.params['page'] = 0;
    $scope.params['startdate'] = '5/10/2016';

    $scope.columns = {};
    for (var i in wifiMappingAPI.apColumns) {
        $scope.columns[wifiMappingAPI.apColumns[i]] = false;
    }
    $scope.columns['ssid'] = true;
    $scope.columns['caps'] = true;

    $scope.execute = function() {
        $scope.text = 'Pending...';

        var columns = [];
        for (var col in $scope.columns) {
            if ($scope.columns[col]) {
                columns.push(col);
            }
        }
        var params = {columns: columns};
        for (param in $scope.params) {
            if ($scope.params[param]) {
                params[param] = $scope.params[param];
            }
        }
        wifiMappingAPI.getAccessPoints(params).then(function(res) {
            $scope.text = res;
        });
    }
});
