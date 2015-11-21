var async = require('async')
  , azure = require('azure-storage')
  , fs = require('fs')
  , request = require('request')
  , Tile = require('geotile')

var appInsights = require('applicationinsights');
appInsights.setup().start();
var appInsightsClient = appInsights.getClient();

var retryOperations = new azure.ExponentialRetryPolicyFilter();

var TILE_SERVER_BASE_URL = 'http://tiles.rhom.io';

function checkTileServer(tileId, callback) {
    var url = TILE_SERVER_BASE_URL + '/tiles/' + tileId;
    request.get(url, { timeout: 15000 }, function(err, httpResponse, body) {
        if (err) return callback(err);
        if (httpResponse.statusCode === 200 && body.indexOf("links") === -1) {
            console.log("got page but didn't have results JSON on it.");
            return callback(null, 404);
        }
        return callback(null, httpResponse.statusCode);
    });
}

function getUnfetchedTile(callback) {
    var url = TILE_SERVER_BASE_URL + '/fetch';
    request.get(url, { timeout: 15000 }, function(err, httpResponse, body) {
        if (err) return callback(err);
        return callback(null, body);
    });
}

function putToTileServer(tileId, location, callback) {

    var attributes = {
        tile_id: tileId,
        locality: location.parsed.locality,
        administrative_area_level_2: location.parsed.administrative_area_level_2,
        administrative_area_level_1: location.parsed.administrative_area_level_1,
        country: location.parsed.country,
        raw: location
    };

    request.post(TILE_SERVER_BASE_URL + "/tiles", {
        timeout: 15000,
        json: {
            data: {
                type: 'tile',
                attributes: attributes
            }
        }
    }, callback);
}

function fetchFromGoogle(tileId, callback) {
    console.log(tileId);
    var tile = Tile.tileFromTileId(tileId);
    console.log(new Date() + ": " + tile.centerLatitude + " , " + tile.centerLongitude);

    var url = "http://maps.google.com/maps/api/geocode/json?latlng=" + tile.centerLatitude + "," + tile.centerLongitude + "&sensor=false";
    console.log('fetching from google');

    request.get(url, { timeout: 15000 }, function(err, httpResponse, body) {
        console.log('fetched from google');
        if (err) {
            console.log('google error: ' + err);
            return setTimeout(function() { callback(err); }, 15 * 1000);
        }

        var json = JSON.parse(body);

        if (json.status !== 'OK' && json.status !== 'ZERO_RESULTS') {
            console.log('json status error: ' + json.status);

            var waitTimeout = 60 * 60 * 1000;
            if (json.status === 'UNKNOWN_ERROR')
	        waitTimeout = 0;
            else
                appInsightsClient.trackMetric("overquota", 1);
            return setTimeout(function() { callback(json.status); }, waitTimeout);
        }

        json.parsed = {};

        if (json.results.length > 0) {
            json.results[0].address_components.forEach(function(component) {
                component.types.forEach(function(type) {
                    if (type === 'locality' || type === 'administrative_area_level_2' ||
                        type === 'administrative_area_level_1' || type === 'country') {
                        json.parsed[type] = component.long_name;
                    }
                });
            });
        }

        return callback(null, json);
   });
}

async.forever(function(next) {
    var startTime = new Date();
    getUnfetchedTile(function(err, tileId) {
        if (err) {
            console.log('getUnfetchedTile err: ' + err);
            return setTimeout(next, 15 * 1000);
        }

        if (tileId === 'empty') {
            console.log('no messages -> next');
            return setTimeout(next, 15 * 1000);
        }

        checkTileServer(tileId, function(err, statusCode) {
            if (err) {
                console.log('checkTileServer err: ' + err);
                return setTimeout(next, 15 * 1000);
            }

            if (statusCode === 200) {
                console.log('already have tile: ' + tileId);
                queueService.deleteMessage(UNFETCHED_QUEUE, message.messageid, message.popreceipt, function(err) {
                    if (err) console.log('deleteMessage err: ' + err);
                });
                return next();
            }

            console.log('fetching from google');

            fetchFromGoogle(tileId, function(err, location) {
                if (err) {
                    console.log('fetch from Google err: ' + err);
                    return setTimeout(next, 15 * 1000);
                }

                putToTileServer(tileId, location, function(err) {
                    if (err) {
                        console.log('putToTileServer err: ' + err);
                        return setTimeout(next, 15 * 1000);
                    }

                    console.dir(location.parsed);

                    appInsightsClient.trackMetric("tile", 1);
                    var checkTime = new Date().getTime() - startTime.getTime();
                    var timeout = Math.max(40 * 1000 - checkTime, 0);
                    setTimeout(next, timeout);
                });
            });
        });
    });
}, function(err) {
    console.log('async exitted: ' + err);
});

setInterval(function() {
    console.log('restarting process.');
    process.exit(0);
}, 1 * 60 * 60 * 1000);
