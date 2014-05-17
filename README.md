# gohaste

Go implementation of a concurrent Rackspace CloudFiles Upload/Download/Delete application

## Binary Downloads

[![Gobuild Download](http://gobuild.io/badge/github.com/sivel/gohaste/download.png)](http://gobuild.io/github.com/sivel/gohaste)

## Usage

```
usage: gohaste [options] {delete,upload,download} source [destination]

delete:
    gohaste [options] delete my-container

upload:
    gohaste [options] upload /path/to/files my-container

download:
    gohaste [options] download my-container /path/to/files

options:
  -concurrency=10: Number of cuncurrent operations. Defaults to 10
  -cpus=1: Number of CPUs to use. Defaults to 1
  -password="": Password to authenticate with. Defaults to OS_PASSWORD
  -region="": Password to authenticate with. Defaults to OS_REGION_NAME
  -username="": Username to authenticate with. Defaults to OS_USERNAME
```
