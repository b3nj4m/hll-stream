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
