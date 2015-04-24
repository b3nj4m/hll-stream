var crypto = require('crypto');
var Stream = require('stream');
var rawEstimateData = require('./rawEstimateData.json');
var biasData = require('./biasData.json');
var thresholdData = require('./thresholdData.json');

var MAX_INT_BITS = 32;
var MAX_INT_BYTES = Math.ceil(MAX_INT_BITS / 8);

function HLL(precision, hashType) {
  Stream.Writable.call(this);

  this.precision = Math.min(16, Math.max(4, precision || 4));
  this.hashType = hashType || 'sha1';
  this.registersSize = 1 << this.precision;
  this.registers = new Array(this.registersSize);
  this.alpha = (this.alphaTable[this.precision] || this.alphaTable.default)(this.precision);

  for (var i = 0; i < this.registers.length; i++) {
    this.registers[i] = 0;
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

HLL.prototype.end = function() {
  this.emit('finish');
};

HLL.prototype.write = function(chunk, enc, next) {
  if (!Buffer.isBuffer(chunk)) {
    chunk = new Buffer(chunk);
  }

  var hash = parseInt(crypto.createHash(this.hashType).update(chunk).digest().slice(0, MAX_INT_BYTES).toString('hex'), 16);
  var idx = hash & (this.registersSize - 1);
  var estimator = hash >> this.precision;

  var estimatorBits = MAX_INT_BITS - this.precision;
  var i;
  var mask = 1 << estimatorBits;

  for (i = 0; i < estimatorBits; i++) {
    if ((estimator & mask) !== 0) {
      break;
    }
    mask = mask >> 1;
  }

  this.registers[idx] = Math.max(this.registers[idx], i + 1);

  if (next) {
    next();
  }

  return true;
};

HLL.prototype.numZeros = function() {
  var zeros = 0;
  for (var i = 0; i < this.registers.length; i++) {
    if (this.registers[i] === 0) {
      zeros++;
    }
  }
  return zeros;
};

HLL.prototype.cardinality = function() {
  var sum = 0;
  for (var i = 0; i < this.registers.length; i++) {
    sum += Math.pow(2, -this.registers[i]);
  }

  var estimate = (this.alpha * this.registersSize * this.registersSize) / sum;

  if (estimate <= 5 * this.registersSize) {
    estimate = estimate - this.estimateBias(estimate);
  }

  var zeros = this.numZeros();
  var thresholdMetric;

  if (zeros > 0) {
    thresholdMetric = this.registersSize * Math.log(this.registersSize / zeros);
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
  
  var closestEstimates = [];
  var estimateVector = rawEstimateData[this.precision - 4];

  for (var i = 0; i < estimateVector.length; i++) {
    closestEstimates.push({val: Math.pow(estimate - estimateVector[i], 2), idx: i});
  }

  closestEstimates = closestEstimates.sort(function(a, b) {
    return a.val - b.val;
  }).slice(0, 5);

  var bias = 0;
  for (var i = 0; i < closestEstimates.length; i++) {
    bias += biasVector[closestEstimates[i].idx];
  }

  return bias / closestEstimates.length;
};

module.exports = HLL;
