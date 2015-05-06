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

        expect(error).to.be.at.most(iterations * (2 / Math.sqrt(h.registersSize)));
        done();
      });

      rs.pipe(h);
    });
  });
}

describe('hll', function() {
  describe('precision', function() {
    it('should have min precision of 4', function() {
      var hll = new HLL(2);
      expect(hll.precision).to.be.equal(4);
    });

    it('should have max precision of 16', function() {
      var hll = new HLL(18);
      expect(hll.precision).to.be.equal(16);
    });
  });

  describe('registers', function() {
    it('should have 2^precision registers', function() {
      var hll = new HLL(4);
      expect(hll.registers.length).to.be.equal(16);
      hll = new HLL(5);
      expect(hll.registers.length).to.be.equal(32);
      hll = new HLL(6);
      expect(hll.registers.length).to.be.equal(64);
      hll = new HLL(8);
      expect(hll.registers.length).to.be.equal(256);
      hll = new HLL(16);
      expect(hll.registers.length).to.be.equal(65536);
    });

    it('should have zeroed registers', function() {
      var hll = new HLL(4);
      for (var i = 0; i < hll.registers.length; i++) {
        expect(hll.registers[i]).to.be.equal(0);
      }
    });
  });

  describe('cardinality', function() {
    for (var i = 4; i < 17; i++) {
      test(i * Math.pow(2, i + 3), Math.max(4, Math.min(16, i)));
    }
  });
});
