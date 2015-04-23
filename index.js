var crypto = require('crypto');
var Stream = require('stream');
var rawEstimateData = require('./rawEstimateData.json');
var biasData = require('./biasData.json');
var thresholdData = require('./thresholdData.json');

var MAX_INT_BITS = 31;
var MAX_INT_BYTES = Math.ceil(MAX_INT_BITS / 8);

function HLL(precision, hashType) {
  Stream.Writable.call(this);

  this.precision = Math.min(16, Math.max(8, precision || 8));
  //TODO bit precision
  this.bytePrecision = Math.ceil(this.precision / 8);
  this.hashType = hashType || 'sha1';
  this.logSize = Math.pow(2, this.precision);
  this.log = new Array(this.logSize);
  this.alpha = (this.alphaTable[this.precision] || this.alphaTable.default)(this.precision);

  for (var i = 0; i < this.log.length; i++) {
    this.log[i] = 0;
  }
}

HLL.prototype = Object.create(Stream.Writable.prototype);
HLL.prototype.constructor = HLL;

HLL.prototype.alphaTable = {
  '4': function() { return 0.673; },
  '5': function() { return 0.697; },
  '6': function() { return 0.709; },
  'default': function(precision) {
    return 0.7213 / (1 + (1.079 / (1 << precision)));
  }
};

HLL.prototype.write = function(chunk, enc, next) {
  if (!Buffer.isBuffer(chunk)) {
    chunk = new Buffer(chunk);
  }

  var hash = crypto.createHash(this.hashType).update(chunk).digest();
  var idx = parseInt(hash.slice(0, this.bytePrecision).toString('hex'), 16);
  var estimator = parseInt(hash.slice(this.bytePrecision, MAX_INT_BYTES).toString('hex'), 16);

  var estimatorBits = MAX_INT_BITS - this.precision;
  var i;
  var mask = Math.pow(2, estimatorBits);

  for (i = 0; i < estimatorBits; i++) {
    if ((estimator & mask) !== 0) {
      break;
    }
    mask = mask >> 1;
  }

  this.log[idx] = Math.max(this.log[idx], i + 1);

  if (next) {
    next();
  }

  return true;
};

HLL.prototype.zeros = function() {
  var zeros = 0;
  for (var i = 0; i < this.log.length; i++) {
    if (this.log[i] === 0) {
      zeros++;
    }
  }
  return zeros;
};

HLL.prototype.cardinality = function() {
  var sum = 0;
  for (i = 0; i < this.log.length; i++) {
    sum += Math.pow(2, -this.log[i]);
  }

  var estimate = (this.alpha * this.logSize * this.logSize) / sum;

  if (estimate <= 5 * this.logSize) {
    return estimate - this.estimateBias(estimate);
  }

  var zeros = this.zeros();
  var thresholdMetric;

  if (zeros > 0) {
    thresholdMetric = this.logSize * Math.log(this.logSize / zeros);
  }
  else {
    thresholdMetric = estimate;
  }

  if (thresholdMetric <= thresholdData[this.precision - 4]) {
    return thresholdMetric;
  }

  return estimate;
};

HLL.prototype.estimateBias = function(estimate) {
  var biasVector = biasData[this.precision - 4];
  
  var closestEstimates = rawEstimateData[this.precision - 4].map(function(estimateValue, i) {
    return {val: Math.pow(estimate - estimateValue, 2), idx: i};
  });

  closestEstimates = closestEstimates.sort(function(a, b) {
    return a.val - b.val;
  }).slice(0, 6);

  var bias = 0;
  for (var i = 0; i < closestEstimates.length; i++) {
    bias += biasVector[closestEstimates[i].idx];
  }

  return bias / closestEstimates.length;
};

module.exports = HLL;
