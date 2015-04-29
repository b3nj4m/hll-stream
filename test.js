var HLL = require('./index');

var expect = require('chai').expect;
var Stream = require('stream');

var totalError = 0;
var totalTests = 0;

function test(iterations, precision) {
  ['buffers', 'strings'].forEach(function(type) {
    it('should count about ' + iterations + ' ' + type + ' using precision ' + precision, function(done) {
      var h = new HLL(precision);
      var rs = new Stream.Readable();
      var i = 0;

      if (type === 'buffers') {
        rs._read = function() {
          var pushed = true;
          //fill buffer
          while (pushed && i < iterations) {
            var buf = new Buffer(4);
            buf.writeInt32LE(i++);
            pushed = this.push(buf);
          }
          if (pushed && i >= iterations) {
            return this.push(null);
          }
        };
      }
      else if (type === 'strings') {
        rs._read = function() {
          var pushed = true;
          //fill buffer
          while (pushed && i < iterations) {
            pushed = this.push((i++).toString());
          }
          if (pushed && i >= iterations) {
            return this.push(null);
          }
        };
      }

      h.on('finish', function() {
        var cardinality = h.cardinality();
        var error = Math.abs(cardinality - iterations);

        totalError += error / iterations;
        totalTests++;

        expect(error).to.be.below(iterations * 0.20);
        done();
      });

      rs.pipe(h);
    });
  });
}

describe('hll', function() {
  for (var i = 1; i < 20; i++) {
    test(i * Math.pow(2, i), Math.max(4, Math.min(16, i + 1)));
  }

  it('should have average error below 5%', function() {
    expect(totalError / totalTests).to.be.below(0.05);
  });
});
