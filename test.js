var HLL = require('./index');

var expect = require('chai').expect;
var Stream = require('stream');

describe('hll', function() {
  it('should count buffers', function(done) {
    var iterations = 1000000;
    var h = new HLL(10);

    var rs = new Stream.Readable();
    var i = 0;
    rs._read = function() {
      if (i < iterations) {
        var buf = new Buffer(4);
        buf.writeInt32LE(i++);
        return this.push(buf);
      }
      else {
        return this.push(null);
      }
    };

    h.on('finish', function() {
      var cardinality = h.cardinality();
      expect(Math.abs(cardinality - iterations)).to.be.below(iterations * 0.1);
      done();
    });

    rs.pipe(h);
  });
  it('should count strings', function(done) {
    var iterations = 1000000;
    var h = new HLL(10);

    var rs = new Stream.Readable();
    var i = 0;
    rs._read = function() {
      if (i < iterations) {
        return this.push((i++).toString());
      }
      else {
        return this.push(null);
      }
    };

    h.on('finish', function() {
      var cardinality = h.cardinality();
      expect(Math.abs(cardinality - iterations)).to.be.below(iterations * 0.1);
      done();
    });

    rs.pipe(h);
  });
});
