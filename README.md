<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

YCSB
====================================


This repository contains the fork of [YCSB](https://ycsb.site) with the following additions:
* Support for [YDB](https://ydb.tech) database
* Multithreaded load phaze (tested with YDB only)

To run YCSB with YDB, we recommend to use our [benchhelpers](https://github.com/ydb-platform/benchhelpers/blob/main/ycsb/README.md).

Here is a prebuild YDB package: [ycsb-ydb-binding-0.18.0-SNAPSHOT.tar.gz](https://storage.yandexcloud.net/ydb-benchmark-builds/ycsb-ydb-binding-0.18.0-SNAPSHOT.tar.gz). It requires Java 13+.

Original YCSB Links
-----
* https://ycsb.site
* [project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

Getting Started
---------------

1. Download the [latest release of YCSB](https://storage.yandexcloud.net/ydb-benchmark-builds/ycsb-ydb-binding-0.18.0-SNAPSHOT.tar.gz):

    ```sh
    curl -O --location https://storage.yandexcloud.net/ydb-benchmark-builds/ycsb-ydb-binding-0.18.0-SNAPSHOT.tar.gz
    tar xfvz ycsb-ydb-binding-0.18.0-SNAPSHOT.tar.gz
    cd ycsb-ydb-binding-0.18.0-SNAPSHOT
    ```

2. Run YCSB command. `threads` option depends on the machine, where you run the YCSB, as well as on YDB setup.

    On Linux:
    ```sh
    YDB_ANONYMOUS_CREDENTIALS=1 ./bin/ycsb.sh load ydb -P workloads/workloada \
        -p dsn=grpc://ydb_host:2135/Root/db1 \
        -threads 10
        -p dropOnInit=true -p import=true

    YDB_ANONYMOUS_CREDENTIALS=1 ./bin/ycsb.sh run ydb -P workloads/workloada \
        -p dsn=grpc://ydb_host:2135/Root/db1 \
        -threads 128
        -p maxexecutiontime=600
    ```

  See benchhelpers [documentation](https://github.com/ydb-platform/benchhelpers/blob/main/ycsb/README.md)
  for a detailed documentation on how to run a workload.

  See [this](https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties) for
  the list of available workload properties.

Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:ydb-binding -am clean package
