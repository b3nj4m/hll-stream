var HLL = require('./index');

var expect = require('chai').expect;
var Stream = require('stream');

function makeHll(iterations, precision, hashType, type, callback) {
  var h = new HLL(precision, hashType);
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
      if (pushed) {
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

  h.on('finish', callback.bind(this, h));

  rs.pipe(h);

  return h;
}

function test(iterations, precision) {
  ['buffers', 'strings'].forEach(function(type) {
    it('should count about ' + iterations + ' ' + type + ' using precision ' + precision, function(done) {
      makeHll(iterations, precision, 'sha1', type, function(h) {
        var cardinality = h.cardinality();
        var error = Math.abs(cardinality - iterations);

        expect(error).to.be.at.most(expectedError(iterations, h.registersSize));
        done();
      });
    });
  });
}

function expectedError(numUnique, registersSize) {
  return numUnique * (2 / Math.sqrt(registersSize));
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

  describe('import/export', function() {
    it('should export', function(done) {
      makeHll(1000, 8, 'md5', 'buffers', function(hll) {
        data = hll.export();
        expect(data.precision).to.equal(hll.precision);
        expect(data.hashType).to.equal(hll.hashType);
        expect(data.registers.length).to.equal(hll.registers.length);
        expect(data.registers).not.to.equal(hll.registers);
        done();
      });
    });
    it('should import exported data', function(done) {
      makeHll(1000, 8, 'md5', 'buffers', function(hll) {
        var newHll = new HLL();
        newHll.import(hll.export());
        expect(newHll.precision).to.equal(hll.precision);
        expect(newHll.hashType).to.equal(hll.hashType);
        expect(newHll.registers.length).to.equal(hll.registers.length);
        expect(newHll.registers).not.to.equal(hll.registers);
        expect(newHll.cardinality()).to.equal(hll.cardinality());
        done();
      });
    });
  });

  describe('merge', function() {
    it('should merge hlls with same elements', function(done) {
      makeHll(1000, 8, 'sha1', 'buffers', function(hll1) {
        makeHll(1000, 8, 'sha1', 'buffers', function(hll2) {
          var hll3 = hll1.merge(hll2);
          expect(hll3.precision).to.equal(hll1.precision);
          expect(hll3.hashType).to.equal(hll1.hashType);
          expect(hll3.registers.length).to.equal(hll1.registers.length);
          expect(hll3.registers).not.to.equal(hll1.registers);
          expect(hll3.registers).not.to.equal(hll2.registers);
          expect(hll3.cardinality()).to.equal(hll1.cardinality());
          expect(hll3.cardinality()).to.equal(hll2.cardinality());
          done();
        });
      });
    });
    it('should merge hlls with different elements', function(done) {
      makeHll(1000, 8, 'sha1', 'buffers', function(hll1) {
        makeHll(1500, 8, 'sha1', 'buffers', function(hll2) {
          var hll3 = hll1.merge(hll2);
          expect(hll3.precision).to.equal(hll1.precision);
          expect(hll3.hashType).to.equal(hll1.hashType);
          expect(hll3.registers.length).to.equal(hll1.registers.length);
          expect(hll3.registers).not.to.equal(hll1.registers);
          expect(hll3.registers).not.to.equal(hll2.registers);
          expect(Math.abs(hll3.cardinality() - 1500)).to.be.at.most(expectedError(1500, hll3.registersSize));
          done();
        });
      });
    });
  });

  describe('cardinality', function() {
    for (var i = 4; i < 17; i++) {
      test(i * Math.pow(2, i + 3), Math.max(4, Math.min(16, i)));
    }
  });
});
