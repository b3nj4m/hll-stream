var HLL = require('./index');

var expect = require('chai').expect;

describe('hll', function() {
  it('should count buffers', function() {
    var h = new HLL();
    for (var i = 0; i < 100000; i++) {
      h.write(new Buffer([i]));
    }
    var cardinality = h.cardinality();
    expect(Math.abs(cardinality - 100000)).to.be.below(1000);
  });
});
