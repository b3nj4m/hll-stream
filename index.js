var crypto = require('crypto');
var Stream = require('stream');
var rawEstimateData = require('./rawEstimateData.json');
var biasData = require('./biasData.json');
var thresholdData = require('./thresholdData.json');

//currently limited to 32-bit hashes because node won't do bit-wise arithmetic with more than 32 bits.
var MAX_INT_BITS = 32;
var MAX_INT_BYTES = Math.ceil(MAX_INT_BITS / 8);

function HLL(precision, hashType, streamOpts) {
  Stream.Writable.call(this, streamOpts);

  this.precision = Math.min(16, Math.max(4, precision || 4));
  this.hashType = hashType || 'sha1';
  this.computeConstants();
  this.registers = new Array(this.registersSize);

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

HLL.prototype.computeConstants = function() {
  this.registersSize = 1 << this.precision;
  this.idxMask = this.registersSize - 1;
  this.estimatorBits = MAX_INT_BITS - this.precision;
  this.alpha = (this.alphaTable[this.precision] || this.alphaTable.default)(this.precision);
  this.nextCounter = 0;
  this.nextLimit = 1000;
};

HLL.prototype._write = function(chunk, enc, next) {
  var hash = crypto.createHash(this.hashType).update(chunk).digest().readIntLE(0, MAX_INT_BYTES);
  var idx = hash & this.idxMask;
  var estimator = hash >>> this.precision;

  this.registers[idx] = Math.max(this.registers[idx], this.estimatorBits - (estimator === 0 ? 0 : Math.ceil(Math.log2(estimator))) + 1);

  if (next) {
    this.nextCounter = (this.nextCounter + 1) % this.nextLimit;
    if (this.nextCounter === 0) {
      setTimeout(next, 0);
    }
    else {
      next();
    }
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
    estimate = thresholdMetric;
  }

  return Math.round(estimate);
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

HLL.prototype.merge = function(hll) {
  if (hll.precision !== this.precision) {
    throw Error('precisions of the HLLs must match');
  }

  if (hll.hashType !== this.hashType) {
    throw Error('hashTypes of the HLLs must match');
  }

  var result = new HLL(this.precision, this.hashType);

  for (var i = 0; i < hll.registers.length; i++) {
    result.registers[i] = Math.max(this.registers[i], hll.registers[i]);
  }

  return result;
};

HLL.prototype.export = function() {
  return {
    hashType: this.hashType,
    precision: this.precision,
    registers: this.registers.slice()
  };
};

HLL.prototype.import = function(data) {
  this.hashType = data.hashType;
  this.precision = data.precision;
  this.registers = data.registers.slice();
  this.computeConstants();
};

module.exports = HLL;
