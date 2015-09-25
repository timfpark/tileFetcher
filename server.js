var async = require('async')
  , azure = require('azure-storage')
  , fs = require('fs')
  , request = require('request')
  , Tile = require('geotile');

var retryOperations = new azure.ExponentialRetryPolicyFilter();
var queueService = azure.createQueueService(
    process.env.AZURE_STORAGE_ACCOUNT, 
    process.env.AZURE_STORAGE_KEY).withFilter(retryOperations);

var UNFETCHED_QUEUE = 'unfetchedtiles';
var TILE_SERVER_BASE_URL = 'http://rhom-tile-service.azurewebsites.net/tiles/';

function checkTileServer(tileId, callback) {
    var url = TILE_SERVER_BASE_URL + tileId;
    request.get(url, function(err, httpResponse, body) {
        if (err) return callback(err);
        return callback(null, httpResponse.statusCode);
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

    console.dir(attributes);
    
    request.post(TILE_SERVER_BASE_URL, {
        json: {
            data: {
                type: 'tile',
                attributes: attributes
            }
        }
    }, callback);
}

function fetchFromGoogle(tileId, callback) {
    var tile = Tile.tileFromTileId(tileId);
    console.log(tile.centerLatitude + " , " + tile.centerLongitude);

    var url = "http://maps.google.com/maps/api/geocode/json?latlng=" + tile.centerLatitude + "," + tile.centerLongitude + "&sensor=false";
    request.get(url, function(err, httpResponse, body) {
        if (err) return callback(err);

        var json = JSON.parse(body);

        if (json.status !== 'OK' && json.status !== 'ZERO_RESULTS') return callback(json.status);

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

queueService.createQueueIfNotExists(UNFETCHED_QUEUE, function(err, result, response) {
	if (err) return console.log(err);
  
    async.forever(function(next) {
        queueService.getMessages(UNFETCHED_QUEUE, function(err, messages) {
            if (err) return next();
            if (messages.length < 1) return next();

            var message = messages[0];
            var tileId = message.messagetext;
            
            queueService.deleteMessage(UNFETCHED_QUEUE, message.messageid, message.popreceipt, function(err) {
                if (err) {
                    console.log(err);
                    return next(err);
                }

                checkTileServer(tileId, function(err, statusCode) {
                    if (err) return next(err);
                    if (statusCode === 200) return next();
                    
                    fetchFromGoogle(tileId, function(err, location) {
                        if (err) return next(err);
                        
                        putToTileServer(tileId, location, function(err) {
                            if (err) return next(err);
    
                            setTimeout(next, 100);
                        });                        
                    });
                });
            });
        });      
    }, function(err) {
            
    });
    
});
