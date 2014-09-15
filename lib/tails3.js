var aws    = require('aws-sdk');
var async  = require('async');
var byline = require('byline');

var Parallel_Limit = 12;
var s3 = new aws.S3();

function getFiles(in_bucket, from, to, with_stage, callback){


    function listWithPrefix(prefix, delimiter, callback){
        return s3.listObjects({
               Bucket: in_bucket,
            Delimiter: delimiter,
               Prefix: prefix
        }, callback);
    }

    function inRange(start, prefix, end){
        // lexicographical comparison of truncated strings suffices
        return prefix >= start.substr(0, prefix.length) && prefix <= end.substr(0, prefix.length);
    }

    function matchPredicate(predicate){
        return function matchNextPart(prefixes, callback){
            if(typeof callback === 'undefined'){
                callback = prefixes;
                prefixes = [''];
            }
            var next_prefixes = [];
            async.eachLimit(prefixes, Parallel_Limit, function(prefix, cb){
                listWithPrefix(prefix, '-', function(err, data){
                    if(err) return cb(err);
                    var matching_prefixes = data.CommonPrefixes
                        .map(function(x){return x.Prefix})
                        .filter(predicate);
                    next_prefixes = next_prefixes.concat(matching_prefixes);
                    cb(null);
                });
            }, function(err){
                if(err) return callback(err);
                next_prefixes.sort();
                callback(null, next_prefixes);
            });
        }
    }

    function getAll(prefixes, callback){
        var all = [];
        async.eachLimit(prefixes, Parallel_Limit, function(prefix, cb){
            listWithPrefix(prefix, undefined, function(err, data){
                if(err) return cb(err);
                var matching_prefixes = data.Contents
                    .map(function(x){return x.Key});
                all = all.concat(matching_prefixes);
                cb(null);
            });
        }, function(err){
            if(err) return callback(err);
            all.sort();
            callback(null, all);
        });
    }

    var matchNextPart = matchPredicate(function(x){
        return inRange(from, x, to);
    });

    var matchStage = matchPredicate(function(x){
        return x.indexOf(with_stage, x.length - with_stage.length - 1) != -1;
    });

    async.waterfall([
        matchNextPart, // match years
        matchNextPart, // match months
        matchNextPart, // match days
        matchNextPart, // match hours
        matchNextPart, // match minutes
        matchStage,    // match the stage
        getAll,        // get everything that matches
    ], callback);
}

function compareTS(a, b){
    if(a.timestamp < b.timestamp){
        return -1;
    }else if(a.timestamp > b.timestamp){
        return 1;
    }else{
        return 0;
    }
}

function getLogDataInRange(bucket, from, to, with_stage, callback){
    var contents = [];
    getFiles(bucket, from, to, with_stage, function(err, files){
        if(err) return callback(err);
        async.eachLimit(files, Parallel_Limit, function(file, cb){
            var stream = byline(s3.getObject({
                Bucket: bucket, 
                   Key: file
            }).createReadStream());

            stream.on('data', function(line){
                contents.push(JSON.parse(line.toString('utf-8')));
            });
            stream.on('end', function(err){
                cb(err);
            });
        }, function(err){
            contents.sort(compareTS);
            callback(err, contents);
        });
    });
}

module.exports = {
           getFiles: getFiles,
  getLogDataInRange: getLogDataInRange
};
