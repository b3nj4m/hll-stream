var HLL = require('./index');

var expect = require('chai').expect;
var Stream = require('stream');

function test(iterations, precision) {
  ['buffers', 'strings'].forEach(function(type) {
    it('should count about ' + iterations + ' ' + type + ' using precision ' + precision, function(done) {
      var h = new HLL(precision);
      var rs = new Stream.Readable();
      var i = 0;

      if (type === 'buffers') {
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
      }
      else if (type === 'strings') {
        rs._read = function() {
          if (i < iterations) {
            return this.push((i++).toString());
          }
          else {
            return this.push(null);
          }
        };
      }

      h.on('finish', function() {
        var cardinality = h.cardinality();
        expect(Math.abs(cardinality - iterations)).to.be.below(iterations * 0.1);
        done();
      });

      rs.pipe(h);
    });
  });
}

describe('hll', function() {
  test(10, 4);
  test(100, 6);
  test(1000, 8);
  test(10000, 8);
  test(100000, 10);
  test(1000000, 12);
  test(10000000, 14);
  test(100000000, 16);
});
