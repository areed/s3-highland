var expect = require('chai').expect;
var _ = require('highland');
var AWS = require('aws-sdk');
AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: 'areedio'});
var service = new AWS.S3({apiVersion: '2006-03-01'});
var S3 = require('../');
var s3 = new S3(service);
var File = require('vinyl');
var prefix = 's3-highland-test';
var index = 0;

describe('s3-highland', function() {
  this.timeout(120000);

  after(function(done) {
    var empty = function(bucket) {
      return _(function(push, next) {
        s3.streamBucketContents({Bucket: bucket})
          .map(s3.deleteObject)
          .series()
          .last()
          .apply(function() {
            push(null, {Bucket: bucket});
            push(null, _.nil);
          });
      });
    };

    s3
      .streamS3Buckets()
      .pluck('Name')
      .filter(function(bucket) {
        return bucket.substring(0, prefix.length) === prefix;
      })
      .flatMap(empty)
      .flatMap(s3.deleteBucket)
      .errors(function(err, rethrow) {
        console.log(err);
      })
      .apply(function() {
        done();
      });
  });

  describe('createBucket', function() {
    var bucket = bucketName();

    it('should end after the bucket is created.', function(done) {
      s3.createBucket({
        Bucket: bucket
      })
      .apply(function(data) {
        expect(data).to.have.property('Location', '/' + bucket);
        done();
      });
    });
  });

  describe('deleteBucket', function() {
    var bucket = prepareBucket();

    describe('given the bucket is empty', function() {
      it('should delete the bucket.', function(done) {
        s3
          .deleteBucket({
            Bucket: bucket
          })
          .errors(done)
          .apply(function(data) {
            expect(data).to.deep.equal({});
            done();
          });
      });
    });
  });

  describe('putObject', function() {
    var bucket = prepareBucket();

    it('should upload the object to S3.', function(done) {
      var file = { Bucket: bucket, Key: 'a', Body: 'A' };

      s3.putObject(file)
        .errors(done)
        .apply(function(o) {
          expect(o).to.have.property('Bucket', bucket);
          expect(o).to.have.property('Key', 'a');
          expect(o).to.have.property('Body', 'A');
          expect(o).to.have.property('ETag');
          done();
        });
    });
  });

  describe('putVinylObject', function() {
    var bucket = prepareBucket();

    it('should upload the object to S3.', function(done) {
      var file = new File({
        base: '/public/',
        path: '/public/index.html',
        contents: Buffer('My Home Page.'),
      });

      file.Bucket = bucket;

      s3.putVinylObject(file)
        .errors(done)
        .apply(function(f) {
          expect(f).to.have.property('Bucket', bucket);
          expect(f).to.have.property('Key', 'index.html');
          expect(f).to.have.property('Body');
          expect(f.Body.toString()).to.equal('My Home Page.');
          expect(f).to.have.property('ETag');
          expect(f).to.have.property('base', '/public/');
          expect(f).to.have.property('path', '/public/index.html');
          expect(f).to.have.property('contents');
          expect(f).to.be.an.instanceof(File);
          done();
        });
    });
  });

  describe('streamBucketContents', function() {
    var bucket = prepareBucket();
    var objects = populateBucket(bucket);

    it('should return a stream containing metadata for each object in the bucket.', function(done) {
      s3.streamBucketContents({Bucket: bucket})
        .toArray(function(contents) {
          expect(contents[0]).to.have.property('Key', objects[0].Key);
          expect(contents[1]).to.have.property('Key', objects[1].Key);
          done();
        });
    });
  });

  describe('deleteObject', function() {
    var bucket = prepareBucket();
    var obj = populateBucket(bucket, 1);

    it('should delete the object from the bucket on S3.', function(done) {
      s3.deleteObject({Bucket: bucket, Key: obj[0].Key})
        .toArray(function() {
          done();
        });
    });
  });

  describe('streamS3Buckets', function() {
    var bucket = prepareBucket();

    it('should list all your buckets.', function(done) {
      var prefix = 's3-highland-test';

      s3.streamS3Buckets()
        .pluck('Name')
        .find(function(b) {
          return b === bucket;
        })
        .toArray(function(buckets) {
          expect(buckets).to.have.length(1);
          done();
        });
    });
  });
});

function bucketName() {
  index++;
  return 's3-highland-test-' + index + '-' + Date.now();
}

function prepareBucket() {
  var bucket = bucketName();

  before(function(done) {
    s3
      .createBucket({Bucket: bucket})
      .errors(done)
      .apply(function() {
        done();
      });
  });

  return bucket;
}

function populateBucket(bucket, count) {
  var files = [
    { Bucket: bucket, Key: 'a', Body: 'apple' },
    { Bucket: bucket, Key: 'b', Body: 'banana' },
  ];

  before(function(done) {
    _(files.slice(0, count || files.length))
      .map(s3.putObject)
      .series()
      .errors(done)
      .toArray(function() {
        done();
      });
  });

  return files;
}
