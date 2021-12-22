```shell
go test -c -o testfs -gcflags "all=-N -l" github.com/ainilili/tdsql-competition/filesort
./testfs -test.run TestFileSorter_Sharding
```
