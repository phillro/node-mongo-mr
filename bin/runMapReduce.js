/**
 * User: philliprosen
 * Date: 8/20/12
 * Time: 4:22 PM
 */

/**
 * Executes map reduce jobs using mongo command objects.
 */
var cli = require('cli'),
  mongoose = require('mongoose'),
  async = require('async');

/**
 Example: node runMapReduce.js -e  production -o output_col -c input_col -q {} -m path_to_map.js -r path_to_reduce.js
 */
cli.parse({
  collection:['c', 'Collection name.', 'string'],
  query:['q', 'query to use on the map reduce', 'string', {}],
  map:['m', 'The map function file to run.', 'string'],
  reduce:['r', 'The map function file to run.', 'string'],
  finalize:['f', 'The map function file to run.', 'string'],
  out:['o', 'The collection to output to', 'string', 'testoutcollection'],
  limit:['l', 'Limit the number of records.', 'Number'],
  sort:['s', 'Sort', 'string', {}],
  env:['e', 'Environment name: development|test|production.', 'string', 'production'],
  config_path:['c', 'Config file path.', 'string', '../etc/conf']
});

cli.main(function (args, options) {
  var conf
  try {
    conf = require(options.config_path)[options.env]
    if (!conf) {
      throw new Exception('Config file not found')
    }
  } catch (ex) {
    cli.fatal('Config file not found. Using: ' + options.config_path)
  }

  var ordrinMongoConnectionString = 'mongodb://' + conf.mongo.user + ':' + conf.mongo.password + '@' + conf.mongo.host + ':' + conf.mongo.port + '/' + conf.mongo.db

  var mapFunction = false;
  var reduceFunction = false;
  var finalizeFunction = false;
  var query = false;
  var out = false;
  var collection = false;
  try {
    if (options.map) {
      mapFunction = require(options.map);
    } else {
      throw 'Missing map function.'
    }
    if (options.reduce) {
      reduceFunction = require(options.reduce);
    } else {
      throw 'Missing reduce function.'
    }
    if (options.out) {
      out = options.out;
    } else {
      throw 'Missing output collection name.'
    }
    if (options.collection) {
      collection = options.collection
    } else {
      throw 'Missing target collection.'
    }

    if (options.query) {
      query = JSON.parse(options.query);
    }

    var command = {
      mapreduce:collection,
      query:query,
      map:mapFunction.toString(),
      reduce:reduceFunction.toString(),
      out:out
    };
    if (options.sort) {
      command.sort = options.sort;
    }
    if (options.finalize) {
      finalizeFunction = require(options.finalize);
      command.finalize = finalizeFunction.toString();
    }
    if (options.limit) {
      command.limit = options.limit;
    }

    console.log('Connecting to ' + ordrinMongoConnectionString);
    mongoose.connect(ordrinMongoConnectionString);
    mongoose.connection.on("open", function () {
      console.log('Connected.');
      console.log('Executing: ');
      console.log(command);
      mongoose.connection.db.executeDbCommand(command, function (err, dbres) {
        if (err) {
          throw err;
        } else {
          console.log('Result: ');
          console.log(dbres)
          process.exit();
        }
      });
    })
  } catch (ex) {
    if (ex) {
      console.log(ex);
    }
    console.log('done.')
    process.exit(1);
  }

})

