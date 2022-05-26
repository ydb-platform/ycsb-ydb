<!--
Copyright (c) 2022 YCSB contributors. All rights reserved.

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

## Quick start

Binding for [YDB](https://www.ydb.tech/), using SQL API
via the [YDB Java SDK](https://github.com/yandex-cloud/ydb-java-sdk).

This section describes how to run YCSB on YDB. YDB database mu

### 1. Start YDB

Install and start YDB. The database must be running, you don't need any preparation steps. By default YCSB will create table named `usertable` with all required fields. Note that if table already exists, it will be dropped.

### 2. Install Java and Maven

### 3. Set Up YCSB

Follow original YCSB docs to build YCSB. No additional configuration required.

### 4. Run YCSB

Now you are ready to run! Load the data:

    ./bin/ycsb load ydb -s -P workloads/workloada > outputLoad.txt

Then the workload:

    ./bin/ycsb run ydb -s -P workloads/workloada > outputLoad.txt

## YDB Configuration Parameters

- `endpoint`
 - This should be an endpoint for YDB database, e.g. `grpc://some.host.net:2135`.
 - No default value, parametr is mandatory.

- `database`
 - Full path to the database, e.g. `/home/mydb`.
 - No default value, parametr is mandatory.