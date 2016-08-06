### tails3: Tail S3 Logs
[![NPM version](https://badge.fury.io/js/tails3.svg)](http://badge.fury.io/js/tails3)

This script prints the most recent log messages from log files in an AWS S3
bucket. It isn't quite real-time, as getting files from S3 involves polling,
but it will list log messages with about a 30 second delay.

It's designed to work with logs produced by
[s3-streamlogger](http://github.com/coggle/s3-streamlogger) and winston.

### Prerequisites
You need to have an AWS S3 bucket with log files using the naming convention
`%Y-%m-%d-%H-%M-<stage>-<hostname>.log`. ("stage" could be any group, but at
Coggle we use it to refer to the groups of servers that are used in production,
testing and development).

### Installation and Usage
```sh
npm install -g tails3
```

```sh
tails3 --bucket=your-log-bucket-name --stage=production
```
`--stage` defaults to production, so if you're viewing production logs you can
leave that option off:
```sh
tails3 --bucket=your-log-bucket-name
```

To filter by regex on the `hostname` property of logged lines:
```sh
tails3 --bucket=your-log-bucket-name --host="^fred[0-9]*$"
```

### License
[ISC](http://opensource.org/licenses/ISC): equivalent to 2-clause BSD.

