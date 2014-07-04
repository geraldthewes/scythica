## Scythica

Utilities to create and manage Scythica S-Datasets.  S-Datasets are meant to be analyzed by 
analytical packages that support memory mapped files, such as the R RScythica package.

See the [RScythica][http://github.com/geraldthewes/RScythica] for details.

Currently supports limited CSV creation.

LICENSE: LGPL 2.1

Features
========

* S Dataframes consists of multiple binary, column oriented files that hold vector data in binary form

* S Dataframes are partioned. 

* Partitions are further broken into large splits, typically 64MB in size for efficiency


Support Types
-------------

The following types are currently supported:

* int32:32-Bit integer 
* double: 64-Bit Double
* factor: Strings interpreted as R factors
* date: Date type

Build Instructions
------------------

Download recent go distrupution from the [golang site](http://golang.org)
Latest build were done using Go 1.3

You will need various version control utilities installed including

* GIT
* Mercurial
* Bazaar

Then:

```
mkdir ~/go   # Or any other path
export GOPATH=~/go
mkdir -p ~/go/src/github.com/geraldthewes/ 
cd ~/go/src/github.com/geraldthewes/
git clone https://github.com/geraldthewes/scythica.git
cd sdsmeta

go get
go build
go install

cd ../sdscreate
go build
go install

sudo cp sdscreate /usr/local/bin
```


How to use
----------


`sdscreate  data/boston.yaml boston.db data/boston-1970-2014.csv`

