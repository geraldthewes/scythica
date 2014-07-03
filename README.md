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



Build Instructions
------------------


