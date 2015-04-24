var HLL = require('./index');

var expect = require('chai').expect;

describe('hll', function() {
  it('should count buffers', function() {
    var iterations = 100000;
    var h = new HLL(4);

    for (var i = 0; i < iterations; i++) {
      h.write(new Buffer([i]));
    }

    var cardinality = h.cardinality();
    expect(Math.abs(cardinality - iterations)).to.be.below(iterations * 0.1);
  });
});
