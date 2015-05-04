Statio-connector-sparkSQL
======================

A crossdata connector to Spark SQL

About
=====

SparkSQL connector for Crossdata.

Requirements
------------

You need to install sbt and maven.

Stratio SparkSQL must be installed and started. [Crossdata] (<https://github.com/Stratio/crossdata>) is needed in order to interact with this connector.

Compiling, building and generating the executable for Stratio SparkSQL Connector
--------------------------------------------------------------------------------
    
    stratio-connector-sparkSQL/	
    
    > sh scripts/installconnector.sh

This connector might be used for querying Parquet HDFS files, so it should use HDFSDatastore, defined as well in [Stratio HDFS][] :

The file called HDFSDataStore.xml contains some properties.

Assuming all HDFS files correspond directly to a Hive table, and we’re using a configured Hive metastore, these parameters should be ignored when HDFS Datastore is being used with SparkSQL Connector.

Running the Stratio SparkSQL Connector
--------------------------------------

To run SparkSQL Connector execute:

    > mvn exec:java -Dexec.mainClass="com.stratio.connector.sparksql.SparkSQLConnector"

Build a redistributable package
-------------------------------

It is possible too, to create a RPM or DEB redistributable package.

RPM Package:

    > mvn unix:package-rpm -N

DEB Package:

    > mvn unix:package-deb -N

Once the package it’s created, execute this commands to install:

RPM Package:

    > rpm -i target/stratio-connector-sparksql-0.1.0-SNAPSHOT.rpm

DEB Package:

    > dpkg -i target/stratio-connector-sparksql-0.1.0-SNAPSHOT.deb

Now to start/stop the connector:

    > service stratio-connector-sparksql start
    > service stratio-connector-sparksql stop

How to use SparkSQL Connector
-----------------------------

A complete tutorial is available [here][]. The basic commands are described below.

1.  Start [crossdata-server and then crossdata-shell][].
2.  <https://github.com/Stratio/crossdata>
3.  Start SparkSQL Connector as it is explained before
4.  In crossdata-shell:

    Add a datastore with this command. We need to specified the XML manifest that defines the data store. The XML manifest can be found in the path of the HDFS Connector in target/stratio-connector-sparksql-0.1.0/conf/HDFSDataStore.xml

        xdsh:user>  ADD DATASTORE <Absolute path to HDFS Datastore manifest>;

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest. Remember that defined options at this manifest will be ignored as SparkSQL Connector doesn’t need them.

        xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'hosts': '', 'user': '', 'path': '', 'highavailability' : ''};

    Add the connector manifest. The XML with the manifest can be found in the path of the SparkSQL Connector in target/stratio-connector-sparksql-0.1.0/conf/SparkSQLConnector.xml

        xdsh:user>  ADD CONNECTOR <Path to SparkSQL Connector Manifest>

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the Connector Manifest.

        xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit':<LimitSize>};

    At this point, we can start to send queries.

        xdsh:user> CREATE CATALOG catalogTest;

        xdsh:user> USE catalogTest;

        xdsh:user> REGISTER TABLE tableTest ON CLUSTER hdfs_prod (id int PRIMARY KEY, name text);

        xdsh:user> SELECT * FROM catalogTest.tableTest;

License
=======

Stratio Crossdata is licensed as [Apache2][]

Licensed to STRATIO (C) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The STRATIO (C) licenses this file to you under the Apache License, Version 2.0 (the “License”); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

[Apache2]: http://www.apache.org/licenses/LICENSE-2.0.txt
