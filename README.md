# mapReduce
A MapReduce implementation in Go!

Inside src, wc.go contains a map reduce implementation of wc. 
```
go run wc.go master sequential pg-*.txt
```

To run sequential tests,

```
go test -v -run Sequential
```

To run parallel tests,
```
go test -v -run Parallel
```

References: https://pdos.csail.mit.edu/6.824/index.html

