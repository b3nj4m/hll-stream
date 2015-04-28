## hll-stream

Pipe in your stream of buffers/strings to get an approximate cardinality (using HyperLogLog).

```javascript
var Hll = require('hll-stream');

var hll = new Hll(8);

//...

myDataSource.pipe(hll);

hll.on('finish', function() {
  console.log(hll.cardinality());
});
```

### Limitations

hll-stream uses 32-bit hash values since node.js won't currently do bit-wise arithmetic on more than 32 bits.

### API

#### Hll(precision, hashType, streamOpts)

Construct a new writable Hll (extends [`Stream.Writable`](https://nodejs.org/api/stream.html#stream_class_stream_writable)).

* `precision` - number of bits of precision (4-16) (default 4). Memory usage is on the order of 2<sup>precision</sup>.
* `hashType` - which hashing algorithm to use on the values. Can be any algorithm supported by [`crypto.createHash`](https://nodejs.org/api/crypto.html#crypto_crypto_createhash_algorithm) (default: `'sha1'`).
* `streamOpts` - the options to pass along to the stream constructor.
 
#### Hll.cardinality()

Compute the approximate cardinality.
