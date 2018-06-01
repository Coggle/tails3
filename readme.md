### tails3: Tail S3 Logs
[![NPM version](https://badge.fury.io/js/tails3.svg)](http://badge.fury.io/js/tails3)

This script prints the most recent log messages from log files in an AWS S3
bucket. It isn't quite real-time, as getting files from S3 involves polling,
but it will list log messages with about a 30 second delay.

It's designed to work with logs produced by
[s3-streamlogger](http://github.com/coggle/s3-streamlogger) and winston.

### Prerequisites
You need to have an AWS S3 bucket with log files using the naming convention
`%Y-%m-%d-%H-%M-(%S-)<stage>-<hostname>.log`. ("stage" could be any group, but at
Coggle we use it to refer to the groups of servers that are used in production,
testing and development).

### Installation and Usage
```sh
npm install -g tails3
```

Ensure that the AWS credentials to access your bucket are [set in your
environment](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-credentials-node.html) 
(for example by setting the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables).

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

To display a specific time range:

```sh
# display log entries since 10th January 2017, 13:24, up to the present (and
# continue showing future entries as they are added)
tails3 --bucket=your-log-bucket-name --since=2017-01-10-13-24

# display log entries since 7th April 2016, 8pm-9pm only, then exit
tails3 --bucket=your-log-bucket-name --since=2016-04-07-20-00 --until=2016-04-07-21-00
```


### License
[ISC](http://opensource.org/licenses/ISC): equivalent to 2-clause BSD.

