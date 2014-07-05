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
* datetime: DateTime type. Format can be specified in the attribute section using the Go Time formating syntax

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

Configuration File
------------------

Configuration files are in YAML format.

First section is columns. For each column specify the column name, column data type and
 optional attributes. 

Column attributes include:

* pkey - Indicates the column is part of the partition key. Multiple columns can form the partition key.
One column must be a partition key
* pkey0p2 - Same as pkey, but column is expected to be a number, and number will be 0 padded to two digits. Useful if you have date fields that need to be part of the key that are not 0 padded
* A go Time datetime format - such as "20060102 15:04"

The keyspace section contain S-Dataset wide configuration settings

* key_size - Unused at the momemnt
* nodes - Unused at the moment
* rows_per_split - Max number of rows to be part of a split. Should be a large number like 1000000 or more.
* isna - Global default for NA value. 

```
columns:
- colname: STATION
  coltype: factor
  attributes: pkey
- colname: STATION_NAME
  coltype: factor
- colname: ELEVATION
  coltype: double
- colname: LATITUDE
  coltype: double
- colname: LONGITUDE
  coltype: double
- colname: DATE
  coltype: datetime
  attributes: "20060102 15:04"
- colname: MEASUREMENT
  coltype: int32
- colname: FLAG 
  coltype: factor
- colname: QUALITY
  coltype: factor
- colname: UNITS
  coltype: factor
keyspace:
  key_size: 8192
  nodes: 1
  rows_per_split: 1000
  isna: "NA"
```

How to use
----------


`sdscreate  data/boston.yaml boston.db data/boston-1970-2014.csv`

