#!/usr/bin/env node


var InfluxCouchDB = require('../lib');

var influx = require('influx');

//Note there is an error in the simple example which i have not tracked down/fixed
//yet it will not restart the stream from where it left off if the feeder is stopped
//
//I am working on the daemon.js in same direcory as this which restarts happily.
//

var settings = 
      {
        couchdb: {
         url: 'http://192.168.3.21:5984',
         influxdb:  'aatest',
         database: 'aatest'
       }
      };


var influxclient = influx({
  host : 'localhost',
  port : 8086, // optional, default 8086
  username : 'root',
  password : 'root',
  database : 'aatest'
});


influxclient.createDatabase('aatest', function(err) {
  if(err){
     console.error('ERROR: Influx create db:', err);
     process.exit();
  }
  console.log('Database Created');
});


	
initial_since = get_initial_since(settings.couchdb.influxdb);  //only returns 0 at the moment

createImporter();


function createImporter(){
    settings.since = initial_since;
    var importer = new InfluxCouchDB(influxclient,  settings );
    
      importer.start();

    //enable what event you want to watch
    importer.events.on('connect', console.log);
    importer.events.on('checkpoint', console.log);
    importer.events.on('checkpoint.error', function(msg, err) {
        console.error(msg, err);
	process.exit(1);
    });

//    importer.events.on('change', console.log);  //very noisy
    importer.events.on('change.error', function(feed, change, err) {
	console.error(feed, err.body, err);
    });

    importer.events.on('error', function(msg, err) { console.error(msg, err); });

    //importer.events.on('drain', console.log);

    importer.events.on('stop', function(key) {
    	console.log(key + ': stopped');
    });

}



function get_initial_since(feedname) {
     // need to get from a file
    initial_since = 0;
}

