/** 
 * A highland streams wrapper around the JavaScript AWS S3 API.
 * @module S3
 */
var _ = require('highland');
var File = require('vinyl');
var extend = require('highlandx/extend');
var pick = require('highlandx/pick');

/** @typedef {Object} HighlandStream */

var pickDeleteObjectParams = _.curry(pick, [
  'Bucket',
  'Key',
  'MFA',
  'VersionId',
]);

var pickPutObjectParams = _.curry(pick, [
  'Bucket',
  'Key',
  'ACL',
  'Body',
  'CacheControl',
  'ContentDisposition',
  'ContentEncoding',
  'ContentLanguage',
  'ContentLength',
  'ContentMD5',
  'ContentType',
  'Expires',
  'GrantFullControl',
  'GrantRead',
  'GrantReadACP',
  'GrantWriteACP',
  'Metadata',
  'SSECustomerAlgorithm',
  'SSECustomerKey',
  'SSECustomerKeyMD5',
  'SSEKMSKeyId',
  'ServerSideEncryption',
  'StorageClass',
  'WebsiteRedirectLocation',
]);

/**
 * @constructor
 * @alias module:S3
 * @param {Object} service - an instance of require('aws-sdk').S3
 */
function S3(service) {

  /**
   * A stream wrapper around the createBucket call.
   * @function
   * @param {Object} params -
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#createBucket-property
   * @return {HighlandStream}
   */
  this.createBucket = _.wrapCallback(service.createBucket.bind(service));

  /**
   * A stream wrapper around the deleteBucket call.
   * @function
   * @param {Object} params -
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteBucket-property
   * @return {HighlandStream}
   */
  this.deleteBucket = _.wrapCallback(service.deleteBucket.bind(service));

  var put = _.wrapCallback(service.putObject.bind(service));

  /**
   * Uploads an object to an S3 bucket. It whitelists the legal params for the
   * call so you can pass in streams from a variety of sources.
   * @function
   * @memberof S3
   * @param {Object} params -
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObject-property
   * @return {HighlandStream} although the S3 call returns an object containing only an
   * ETag, the object in this stream will also have all passed-in parameters for
   * convenient piping.
   */
  var putObject = this.putObject = function(params) {
    return put(pickPutObjectParams(params))
      .map(_.extend(params));
  };

  var del = _.wrapCallback(service.deleteObject.bind(service));

  /**
   * Deletes an object from an S3 bucket. The bucket must be empty. Whitelists
   * legal params for the call so you can pass in streams from a variety of
   * sources.
   * @param {Object} params -
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#deleteObject-property
   * @return {HighlandStream} will consist of one item: the original params 
   * extended with any properties returned from S3 (DeleteMarker and VersionId).
   */
  this.deleteObject = function(params) {
    return del(pickDeleteObjectParams(params))
      .map(_.extend(params));
  };

  /**
   * Calculates and whitelists allowed parameters from a Vinyl virtual file
   * object and then uploads it to an S3 bucket.
   * @param {Object} file - Must have a Bucket property. Body will be taken from
   * the contents property and Key will be calculated from base and path
   * properties if needed.
   * @return {HighlandStream} contains one item: the original file extended with
   * Body and Etag properties.
   */
  this.putVinylObject = function(file) {
    var o = _.extend({
      Key: key(file),
      Body: file.contents,
    }, pickPutObjectParams(file));

    return putObject(o)
      .map(function(etagged) {
        return _.extend(etagged, file);
      });
  };

  /**
   * Like listObjects but returns a stream where each item is an object in the
   * bucket.
   * @param {Object} params - can contain any legal parameters for listObjects.
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listObjects-property
   * @return {HighlandStream}
   */
  this.streamBucketContents = function(params) {
    var objs = _();

    service
      .listObjects(params)
      .eachItem(function(err, data) {
        if (err) {
          objs.write(err);
          return;
        }
        if (data === null) {
          objs.end();
          return;
        }
        objs.write(_.extend({Bucket: params.Bucket}, data));
      });
 
    return objs;
  };

  /**
   * A stream wrapper around the listBuckets call.
   * http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#listBuckets-property
   * @return {HighlandStream} a stream of the data.Buckets array (data.Owner is
   * discarded). Each item will have Name and CreationDate properties, so map
   * the Name property to Bucket property with nameToBucket before passing the
   * stream to deleteBucket or streamBucketContents.
   */
  this.streamS3Buckets = function() {
    var buckets = _();

    service
      .listBuckets()
      .eachItem(function(err, data) {
        if (err) {
          buckets.write(err);
          return;
        }
        if (data === null) {
          buckets.end();
          return;
        }
        buckets.write(data);
      });

    return buckets;
  };

}

/**
 * {Name: 'my-bucket', CreationDate: XXXX} => {Bucket: 'my-bucket'};
 * Useful for mapping over the stream returned from streamS3Buckets before
 * passing to calls that expect a Bucket param.
 * @param {Object} bucket - should have a "Name" property
 * @return {Object} will have a single property "Bucket"
 */
S3.prototype.nameToBucket = function(bucket) {
  return {Bucket: bucket.Name};
};

module.exports = S3;

function key(file) {
  if (file.Key) {
    return file.Key;
  }
  if (file.path.indexOf(file.base) !== 0 || file.path.length === file.base.length) {
    throw new Error('Cannot calculate Key from base and path.');
  }
  return file.path.substring(file.base.length);
}
