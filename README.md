## hll-stream

Pipe in your stream of buffers/ints to get an approximate cardinality.

```javascript
var Hll = require('hll-stream');

var hll = new Hll(8);

//...

myDataSource.pipe(hll);

hll.on('end', function() {
  console.log(hll.cardinality());
});
```
