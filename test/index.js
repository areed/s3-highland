var AWS = require('aws-sdk');
var expect = require('chai').expect;
var _ = require('highland');

var _s3 = require('../index');

var AccessKeyID = process.env.BUCKET_STREAM_ACCESS_KEY_ID;
var SecretAccessKey = process.env.BUCKET_STREAM_SECRET_ACCESS_KEY;
var creds = {
  accessKeyId: AccessKeyID,
  secretAccessKey: SecretAccessKey
};
var s3 = new AWS.S3(_.extend({apiVersion: '2006-03-01'}, creds));

describe('listObjects', function() {
  var count = 30;
  var parallelism = 3;
  var bucket;

  this.timeout(count * 2500);

  before(function(done) {
    var populateX = _.curry(populate, count);

    getTestBucket()
    .flatMap(populateX)
    .map(_s3.putObject)
    .parallel(parallelism)
    .toArray(function(objs) {
      bucket = objs[0].Bucket;
      done();
    });
  });

  after(function(done) {
    if (bucket) {
      disposeBucket(bucket)
      .toArray(function() {
        done();
      });
      return;
    }
    done(null);
  });

  it('should list all keys in the bucket', function(done) {
    var index = 0;
    var seen = {};

    _s3.listObjects(s3, {Bucket: bucket}).each(function(s3Obj) {
      expect(s3Obj).to.have.property('Key');
      expect(s3Obj.Key).to.have.length.above(7);
      expect(seen[s3Obj.Key]).to.equal(undefined);
      seen[s3Obj.Key] = true;
      expect(s3Obj).to.have.property('Body', null);
      index++;
      if (index === count) done();
    });
  });
});

function getTestBucket() {
  var bucket = 's3-highland-test-' + new Date().valueOf();
  return _s3.createBucket(s3, {Bucket: bucket}).map(function(bucket) {
    return bucket.Location.slice(1);
  });
}

function disposeBucket(bucket) {
  return emptyBucket(bucket)
  .reduce1(noop)
  .flatMap(function() {
    return _s3.deleteBucket(s3, {Bucket: bucket});
  });
}

function emptyBucket(bucket) {
  return _s3.listObjects(s3, {Bucket: bucket})
  .map(_s3.deleteObject)
  .parallel(3);
}

function loremKey(index) {
  return 'itemKey' + index;
}

function loremBody(index) {
  return 'body' + index;
}

function dummy(bucket, index) {
  return _s3.newS3Object(s3, {
    Bucket: bucket,
    Key: loremKey(index),
    Body: loremBody(index)
  });
}

function populate(count, bucket) {
  return _(function(push, next) {
    for (var i = 0; i < count; i++) {
      push(null, dummy(bucket, i));
    }
    push(null, _.nil);
  });
}

function noop() {}
