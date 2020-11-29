# MyUploader

Streaming upload [mydumper](https://github.com/maxbube/mydumper) output sql file objects to [minio](https://github.com/minio/minio)

## Build

```
$ go build -o myuploader .
```

## Config

```
{
    "directory": "./export-20201129-080140",
    "scanIntervalSec": 1, 
    "queueSize": 100,
    "minio": {
        "endpoint": "127.0.0.1:9000",
        "bucket": "polym.xyz",
        "prefix": "xxx",
        "key": "AKIAIOSFODNN7EXAMPLE",
        "secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    }
}
```

## Run

```
$ ./myuploader -conf config.json
```