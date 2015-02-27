var follow = require('follow');
var EventEmitter = require('events').EventEmitter;
var async = require('async');
var Q = require('kew');
var fs = require('fs');


function InfluxCouchDB(influxclient, options) {

        var opts = set_options(options);
        var events = new EventEmitter();
	
        var changecount = opts.couchdb.since;
        if(changecount < 0) changecount = 0;
        var previous_since = changecount;

//console.error('start changecount', opts.couchdb.since, changecount, previous_since);

        var alive = true;
        var status = '';

        function set_options(opts) {
            opts = opts || {};
            opts.couchdb = opts.couchdb || {};
            return {
                couchdb: {
                    url: opts.couchdb.url || process.env.COUCHDB_URL || 'http://localhost:5984',
                    username: opts.couchdb.username || process.env.COUCHDB_USERNAME,
                    password: opts.couchdb.password || process.env.COUCHDB_PASSWORD,
                    database: opts.couchdb.database || process.env.COUCHDB_DATABASE,
                    since: opts.couchdb.since || 0,
                    influxdb: opts.couchdb.influxdb || process.env.INFLUXDB
                },
                map: opts.map || null
            };
        }
        var influxdb = opts.couchdb.influxdb;
    

        var queue = async.queue(process_change, 1);
        queue.drain = function() {
            events.emit('drain', opts.couchdb.influxdb + ': drain');
        };

        var db_url = [opts.couchdb.url, opts.couchdb.database].join('/');
        var auth;
        if (opts.couchdb.username && opts.couchdb.password) {
            auth = 'Basic ' + new Buffer([opts.couchdb.username, opts.couchdb.password].join(':')).toString('base64');
        }
        var stream = new follow.Feed({
            db: [opts.couchdb.url, opts.couchdb.database].join('/'),
            include_docs: true
        });


	setTimeout(function() {
 	    events.emit('checkpoint', opts.couchdb.influxdb + ': Starting checkpointer');
            checkpoint_changes(changecount)  
	}, 1000);


        function checkpoint_changes(last_changecount) {
	    var ckwait = 3 * 1000;
	    if(alive){

//console.error('in checkpoint_changes', last_changecount, changecount);

		if(last_changecount < changecount){

                    fs.writeFile(influxdb + '_checkpoint', changecount, function(err, result) {
        	        if (err) {
                	    events.emit('checkpoint.error', influxdb + ": UNABLE TO SET SINCE CHECKPOINT to " + changecount, err);
	                } else {
	                    events.emit('checkpoint', influxdb + ': Checkpoint set to ' + changecount + ' next check in ' +  (ckwait / 1000) + " seconds");
	                }
	            });
		}else{
	      //       ckwait = Math.floor(Math.random() * ((60000*2) - 30000) + 30000)
		     ckwait = 10 * 1000; //increase wait as may be idle for a while - adjust to suite needs
	             events.emit('checkpoint', influxdb + ": Checkpoint " + changecount + ' is current next check in: ' +  Math.floor(ckwait / 1000) + ' seconds');
		}
		previous_since = changecount; //for /_status
		setTimeout(function() {
			checkpoint_changes(previous_since)
		     }, ckwait);
	    }
        }

        function update(influxdb, key, doc) {
   
           //we are only going to insert records into influx and dont care about updates
  
            var deferred = Q.defer();
	    //var type;
            var value;
            if ((doc.type == "counter") || (doc.type == "gauge")){
		var series = doc.name;
		switch(doc.type){
			case 'counter':
				value = doc.count;
         			break;
 			case 'gauge':
				value = doc.value;
				break;
			default:
	                        deferred.reject('Unknown metric type: ' + doc.type);		
				return deferred.promise;

		}

/*
		var payload = {
		    		name: doc.name,
		   	 	columns: doc.type,
		    		points: []
	   	    	      };
*/
		var point = { value: value, time: doc.ts };

          
                 influxclient.writePoint(doc.name, point,  function(err) {



                   if (err) {
                      //console.error(influxdb + ": " + ' ' + sql, err);
                      deferred.reject('xx' + err);
                    } else {
                        console.log(influxdb + ": " + doc._id + " added");
                      deferred.resolve(doc._id);
                    }

                 });
            } else {
                //console.log(influxdb + ": " + doc._id + " is " + doc.type + "ignoring");
                deferred.resolve('ignoring');
            }

            return deferred.promise;
        }


        function destroy(influxdb, key) {
            var deferred = Q.defer();
            //We are only inserting so lets just ignore this
            deferred.resolve('nothing to delete');
            return deferred.promise;
        }

        function process_change(change, done) {
            var promise;
            var deletion = !!change.deleted;
            var doc = change.doc;

            if (opts.map) {
                doc = opts.map(doc);
            }

            if (deletion) {
                promise = destroy(opts.couchdb.influxdb, change.id);
            } else {
                promise = update(opts.couchdb.influxdb, change.id, doc);
            }
            promise
                .then(function(res, body) {
                    events.emit('change', change);
                    events.emit('change.success', change);
                    done();
                })
                .fail(function(err) {

               if (err.code == "EMYEXAMPLE") { 
                    console.error('Error: example error in change');
                    status = 'Error: example';
                    stopFollowing();                    
               } else if (err.code == 'ECONNREFUSED') { //couchdb error
                    status = 'Error: Not connected to couch server trying to reconnect.';
                    wait = Math.floor(Math.random() * (60000 - 10000) + 10000); //mixup wait time as could be many 
                    console.error('Error ECONNREFUSED:' + opts.couchdb.influxdb + ' in change');
                    setTimeout(function() {
                        stream.restart();
                    }, wait);
                 }
                  events.emit('change', change, err);
                  events.emit('change.error', opts.couchdb.influxdb, change, err);
                  done(err);
              });

            changecount++;
        }


        function startFollowing() {

            if (auth) stream.headers.Authentication = auth;

            stream.since = previous_since;
            stream.inactivity_ms = 30000;
            // stream.heartbeat = 10000;

            stream.on('confirm', function(db_info) {
                //console.log(JSON.stringify(db_info));
                events.emit('connect', opts.couchdb.influxdb + ': ' + JSON.stringify(db_info));
                status = "Following";
            });
            stream.on('change', function(change) {
                events.emit('change', opts.couchdb.influxdb + ':' + JSON.stringify(change));
                events.emit('change.start', opts.couchdb.influxdb + ': ' + JSON.stringify(change));
                // pause the stream
                stream.pause();
                queue.push(change, function() {
                    // unpause the stream
                    stream.resume();
                });
            });
            stream.on('error', function(err) {
                if (err.code == 'ECONNREFUSED') { //couchdb error
                    status = 'Error: Not connected to couch server trying to reconnect.';
                    wait = Math.floor(Math.random() * (60000 - 10000) + 10000); //mixup wait time as could be many 
                    events.emit('error', opts.couchdb.influxdb + ': Error connection refused. Sleeping for: ' + Math.floor(wait / 1000) + ' seconds', err);
                    setTimeout(function() {
                        stream.restart();
                    }, wait);
                } else if (err.toString().indexOf("no_db_file") > 0) { //couchdb error
                    status = 'Error: db not found on couch server';
                    events.emit('error', opts.couchdb.influxdb + ': couchdb not found', err);
                    stopFollowing();
                } else {
                    status = 'unknown';
                    events.emit('error', opts.couchdb.influxdb + ': stream.on error #' + err + '#', err);
                }
            });

            stream.follow();
            //  events.stop = stream.stop.bind(stream);
            //  return events;

	   //TODO: set started_on -like couch rep status page
           //      also last update?
        }


        function stopFollowing() {
            console.log(opts.couchdb.pgtable + ': stopping stream');
            stream.stop();
            stream.removeAllListeners();
            events.emit('stop', opts.couchdb.pgtable);
            events.removeAllListeners();
            status = 'stopped';
            alive = false;
        }

	



        function is_alive() {
            return alive;
        }
        function get_status() {
            return status;
        }
        function get_since () {
            return changecount;
	}
        function get_since_checkpoint () {
            return previous_since;
	}


        return {
            events: events,
            alive: function alive() {
                return is_alive()
            },
            status: function () {
                return get_status()
            },
            since: function () {
                return get_since()
            },
            since_checkpoint: function () {
                return get_since_checkpoint()
            },
            start: function start() {
                return startFollowing()
            },
            stop: function stop() {
                return stopFollowing()
            }
        };

    }

module.exports = InfluxCouchDB;


