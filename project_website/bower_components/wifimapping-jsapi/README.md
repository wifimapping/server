[![Stories in Ready](https://badge.waffle.io/wifimapping/app.png?label=ready&title=Ready)](http://waffle.io/wifimapping/app)

# Wifimapping Javascript API

This is a javascript wrapper on the web API.  

# Installation

        bower install --save https://github.com/docmarionum1/jsapi.git

# Usage

In your HTML file include the script:

    <script src="wifimappingapi.js"></script>

In your javascript code:

    angular
    .module('App', ['WifiMapping'])
    .controller('Ctrl', function(wifiMappingAPI) {

        wifiMappingAPI.query({
             ssid: 'nyu'   
        }).then(function(res) {
            //Do stuff
        });
    });

# Demo

The `demo` directory contains an example of using the library.  It can easily
be run with:

    # Python 2
    $ python -m SimpleHTTPServer
    # Python 3
    $ python -m http.server

And then navigating to `http://localhost:8000/demo/` in a browser.

# Functions

## wifiMappingAPI.query()

Query the API for individual scans.  Takes an object with the following
parameters:

* `page_size`: Number of responses
* `page`: when using page size, which page to return.
    Starts at 0.
* `columns`: a list of columns to return.  Available columns are:
    * `idx`
    * `lat`
    * `lng`
    * `acc`
    * `altitude`
    * `time`
    * `device_mac`
    * `app_version`
    * `droid_version`
    * `device_model`
    * `ssid`
    * `bssid`
    * `caps`
    * `level`
    * `freq`
* `acc`: accuracy greater than or equal to the given value
* `startdate`: date grater than or equal to this date
* `enddate`: date less than this date
* `device_mac`: scan done by the given device mac address
* `app_version`: scan done by given app version
* `droid_version`: scan done by phone with given android version
* `device_model`: scan done by phone of given model
* `ssid`: a single ssid or list of ssids
* `bssid`: equal to the given bssid
* `caps`: contains a given capability. i.e. `wps`
* `level`: greater than or equal to the given signal strength
* `freq`: Equal to the specified frequency

Returns a list of scan results with the columns specified.

## wifiMappingAPI.getAccessPoints()

Query the API for unique access points.  Takes an object with the following
parameters:

* `page_size`: Number of responses
* `page`: when using page size, which page to return.
    Starts at 0.
* `columns`: a list of columns to return.  Available columns are:
    * `ssid`
    * `bssid`
    * `caps`
    * `freq`
* `acc`: accuracy greater than or equal to the given value
* `startdate`: date grater than or equal to this date
* `enddate`: date less than this date
* `device_mac`: scan done by the given device mac address
* `app_version`: scan done by given app version
* `droid_version`: scan done by phone with given android version
* `device_model`: scan done by phone of given model
* `ssid`: a single ssid or list of ssids
* `bssid`: equal to the given bssid
* `caps`: contains a given capability. i.e. `wps`
* `level`: greater than or equal to the given signal strength
* `freq`: Equal to the specified frequency

Returns a list of access points with the columns specified.

## wifiMappingAPI.queryParams

The list of available parameters.

## wifiMappingAPI.queryColumns

The list of columns for `query`.

## wifiMappingAPI.apColumns

The list of columns for `getAccessPoints`.
