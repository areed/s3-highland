var _ = require('highland');
var pick = require('highlandx/pick');

/**
 * Creates a new virtual s3Object.
 * @class
 * @param{Object} s3service - the service object to use to read or write this file
 * on S3. It should be an instance of AWS.S3
 * @param{Object} attrs - the virtual s3 object attributes
 */
function S3Object(s3service, attrs) {
  return _.extend(attrs, {
    s3: s3service,
    Bucket: '',
    Key: '',
    Body: null
  });
}

/**
 * Curryable wrapper of the constructor.
 */
var newS3Object = exports.newS3Object = function(s3service, attrs) {
  return new S3Object(s3service, attrs);
};

/**
 * Higher-order function to wrap s3 methods that don't require any
 * modifications.
 * @param{String} methodKey - name of the method to wrap
 */
function wrap(methodKey) {
  return function(s3service, params) {
    return _.wrapCallback(s3service[methodKey].bind(s3service))(params);
  };
}

//all the methods that can be wrapped simply with the wrap function
[
  'createBucket',
  'deleteBucket'
].forEach(function(methodKey) {
  exports[methodKey] = wrap(methodKey);
});

/**
 * Higher-order function to wrap s3 methods that operate on objects from a
 * stream.
 * @param {string} methodKey - name of the method to wrap
 * @param {string[]} paramKeys - allowed params for the method
 * @return {object} a stream
 */
function adapt(methodKey, paramKeys) {
  var sanitizeParams = _.curry(pick, paramKeys);

  return function(s3Obj) {
    var s3Service = s3Obj.s3;
    var call = _.wrapCallback(s3Service[methodKey].bind(s3Service));

    return sanitizeParams(s3Obj)
    .map(function(p) {
      return p;
    })
    .map(call)
    .series()
    .map(function(data) {
      return _.extend(data, s3Obj);
    });
  };
}

exports.deleteObject = adapt('deleteObject', [
  'Bucket',
  'Key',
]);

exports.putObject = function(s3Obj) {
  var params = _.curry(pick, [
    'ACL',
    'Body',
    'Bucket',
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
    'Key',
    'Metadata',
    'ServerSideEncryption',
    'StorageClass',
    'WebsiteRedirectLocation',
    'SSECustomerAlgorithm',
    'SSECustomerKey',
    'SSECustomerKeyMD5'
  ]);

  return _(function(push, next) {
    params(s3Obj).each(function(params) {
      s3Obj.s3.putObject(params, function(err, data) {
        if (err) {
          push(err, data);
        } else {
          push(null, s3Obj);
        }
        push(null, _.nil);
      });
    });
  });
};

/**
 * Lists all the keys in a bucket and handles paging transparently.
 * @param{Object} s3service - the service object to use to read or write this file
 * on S3. It should be an instance of AWS.S3
 * @param{Object} params - Bucket is required
 * be located
 * @return{Object} stream of s3Objects with empty body attributes
 */
exports.listObjects = function(s3service, params) {
  return _(function(push, next) {
    var s3ObjFactory = _.curry(newS3Object, s3service);

    s3service.listObjects(params).eachItem(function(err, data) {
      if (err) {
        push(err);
        push(null, _.nil);
        return;
      }
      //data will be null when all items have been listed
      if (data === null) {
        push(null, _.nil);
        return;
      }
      push(null, s3ObjFactory(_.extend({Bucket: params.Bucket}, data)));
    });
  });
};
