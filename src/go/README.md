### WMArchive service
This area contains WMArchive service implementation in Go language.
To build the code you need a Go compiler and then perform the following steps:
```
# get required dependencies
go get github.com/go-stomp/stomp
go get github.com/google/uuid
go get github.com/lestrrat-go/file-rotatelogs
go get github.com/nats-io/nats.go

# build server code
make

# run server code
./wmarchive -config config.json
```

### Testing procedure
To test the code we'll use plain curl call
```
# get status of the service
curl http://localhost:8200/wmarchive/data/

# test POST request
curl -X POST -H "Content-type: application/json" -d@doc.json http://localhost:8200/wmarchive/data/
{"result":[{"ids":["ce3f0a9a497848628d929a78363ad5ab"],"status":"ok"}]}
```
Here, the `doc.json` represent WMArchive document.
