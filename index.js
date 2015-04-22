var crypto = require('crypto');
var Stream = require('stream');

var alpha = 0.709;

function HLL(precision, hashType) {
  Stream.Writable.call(this);

  this.precision = precision || 4;
  this.bytePrecision = Math.ceil(this.precision / 4);
  this.hashType = hashType || 'sha1';
  this.log = new Map();
}

HLL.prototype = Object.create(Stream.Writable.prototype);
HLL.prototype.constructor = HLL;

HLL.prototype.write = function(chunk, enc, next) {
  var hash = crypto.createHash(this.hashType);

  if (!Buffer.isBuffer(chunk)) {
    chunk = new Buffer(chunk);
  }

  var key = hash.update(chunk).digest().slice(0, this.bytePrecision).toString('hex');
  this.log.set(key, true);

  if (next) {
    next();
  }

  return true;
};

HLL.prototype.cardinality = function() {
  return this.log.size;
};

module.exports = HLL;
