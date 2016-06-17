var URL = 'http://capstone.cloudapp.net/wifipulling/';

// Mapping of param names/columns from new to old
// Decided not to change names of columns yet, don't want to have to
// convert them all on the response
var MAPPING = {
    'page_size': 'batch',
    'page': 'offset',
    //'accuracy': 'acc',
    //'capabilities': 'caps',
    //'signal_strength': 'level',
    //'frequency': 'freq',
    //'lon': 'lng'
};

// List of valid parameters
var PARAMS = [
    'page_size', 'page', 'acc', 'altitude', 'startdate',
    'enddate', 'device_mac', 'app_version', 'droid_version',
    'bssid', 'caps', 'level', 'freq',
    'columns', 'ssid',
];

// List of valid columns for query
var QUERY_COLUMNS = [
    'lat', 'lng', 'acc', 'altitude', 'time', 'device_mac',
    'app_version', 'droid_version', 'device_model', 'ssid', 'bssid',
    'caps', 'level', 'freq'
];

// List of valid columns for getAccessPoints
var AP_COLUMNS = [
    'ssid', 'bssid', 'caps', 'freq'
];

/*
    Get a pipe separated list of columns to request be returned
 */
function getRequestColumns(validColumns, params) {
    if (params.columns) {
        var columns = [];
        for (i in validColumns) {
            var column = validColumns[i];

            if (params.columns.indexOf(column) >= 0) {
                if (column in MAPPING) {
                    columns.push(MAPPING[column]);
                } else {
                    columns.push(column);
                }
            }
        }
        return columns.join('|');
    }
}

/*
    Get a dictionary with the request parameters formatted for the wifipulling
    API.
 */
function getRequestParams(validParams, validColumns, params) {
    // Convert ssid to | separated list if an array
    var ssid = params.ssid;
    if (ssid & Array.isArray(params.ssid)) {
        ssid = params.ssid.join('|');
    }

    queryParams = {
        ssid: ssid,
        columns: getRequestColumns(validColumns, params),
    };

    for (param in params) {
        if (['ssid', 'columns'].indexOf(param) >= 0) {
            continue;
        }
        
        if (validParams.indexOf(param) >= 0) {
            if (param in MAPPING) {
                queryParams[MAPPING[param]] = params[param];
            } else {
                queryParams[param] = params[param];
            }
        }
    }

    return queryParams;
}

angular
.module('WifiMapping', [])
.provider('wifiMappingAPI', function() {
    this.$get = ['$http', function($http) {
        return {

            queryParams: PARAMS,
            queryColumns: QUERY_COLUMNS,
            apColumns: AP_COLUMNS,

            /*
                Query the API for individual scans.  Accepts an object `params`
                with the following properties:

                    * page_size: Number of responses
                    * page: when using page size, which page to return.
                        Starts at 0.
                    * columns: a list of columns to return.  Available columns
                        are: idx, lat, lng, acc, altitude, time, device_mac
                        app_version, droid_version, device_model, ssid, bssid,
                        caps, level and freq.
                    * acc: accuracy greater than or equal to the given value
                    * startdate: date grater than or equal to this date
                    * enddate: date less than this date
                    * device_mac: scan done by the given device mac address
                    * app_version: scan done by given app version
                    * droid_version: scan done by phone with given android version
                    * device_model: scan done by phone of given model
                    * ssid: a single ssid or list of ssids
                    * bssid: equal to the given bssid
                    * caps: contains a given capability. i.e. `wps`
                    * level: greater than or equal to the given
                        signal strength
                    * freq: Equal to the specified frequency
             */
            query: function(params) {
                return $http.get(URL, {
                    params: getRequestParams(PARAMS, QUERY_COLUMNS, params)
                }).then(function(res) {
                    return res.data;
                });
            },

            /*
                Query the API for unique access points.
                Accepts an object `params` with the following properties:

                    * page_size: Number of responses
                    * page: when using page size, which page to return.
                        Starts at 0.
                    * columns: a list of columns to return.  Available columns
                        are: ssid, bssid, caps, and freq.
                    * acc: accuracy greater than or equal to the given value
                    * startdate: date grater than or equal to this date
                    * enddate: date less than this date
                    * device_mac: scan done by the given device mac address
                    * app_version: scan done by given app version
                    * droid_version: scan done by phone with given android version
                    * device_model: scan done by phone of given model
                    * ssid: a single ssid or list of ssids
                    * bssid: equal to the given bssid
                    * caps: contains a given capability. i.e. `wps`
                    * level: greater than or equal to the given
                        signal strength
                    * freq: Equal to the specified frequency
             */
            getAccessPoints: function(params) {
                var queryParams = getRequestParams(PARAMS, AP_COLUMNS, params);
                queryParams['distinct'] = 1;
                return $http.get(URL, {
                    params: queryParams
                }).then(function(res) {
                    return res.data;
                });
            }
        }
    }];
});
