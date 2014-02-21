(function () {
  var async = require('async'),
      pg = require('pg'),
      topojson = require('topojson');

  var postGeo = {};

  postGeo.connect = function(credentials) {
    this.client = new pg.Client(credentials);

    // Connect to postgres
    this.client.connect();
  }

  postGeo.query = function(query, format, callback) {
    async.waterfall([
      function(callback) {
        postGeo.client.query(query, function(error, result) {

          callback(null, {"geometry": result.rows});
        });
      },
      function(data, callback) {
        var output = {"type": "FeatureCollection", "features": []};

        async.each(data.geometry, function(row, geomCallback) {
          var parsedRow = {"type": "Feature", "geometry": JSON.parse(row.geometry)};

          if (Object.keys(row).length > 1) {
            parsedRow.properties = {};
            async.each(Object.keys(row), function(property, propCallback) {
              if (property != "geometry") {
                parsedRow.properties[property] = row[property];
              }
              propCallback();
            }, function(error) {
              output.features.push(parsedRow)
              geomCallback();
            });
          } else {
            output.features.push(parsedRow)
            geomCallback();
          }
          
        },
        function(err) {
          if (err) throw err;
          if (format === "topojson") {
            callback(null, topojson.topology({output: output}));
          } else {
            callback(null, output);
          }
        });
      },
      function(data) {
        callback(data);
      }
    ]);
  }

  module.exports = postGeo;
})();