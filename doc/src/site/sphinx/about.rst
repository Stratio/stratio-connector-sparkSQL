About
=====
The Stratio Connector-SparkSQL allows `Stratio Crossdata <https://github.com/Stratio/crossdata>`__ to interact with Spark SQL (Apache Spark).

Requirements
------------
- You need to install sbt and maven.

- `Stratio Crossdata <https://github.com/Stratio/crossdata>`__ is needed in order to interact with this connector.

- An existing and deployed `Hive metastore <https://hive.apache.org/>`__.

Compiling, building and generating the executable for Stratio Connector-SparkSQL
--------------------------------------------------------------------------------
In the stratio-connector-sparkSQL/ directory:

::

    > sh scripts/installconnector.sh

This connector might be used for querying Parquet HDFS files, so it should use HDFSDatastore, defined as well in `Stratio HDFS <https://github.com/Stratio/stratio-connector-hdfs/tree/master/connector-hdfs/src/main/config>`__:

The file called HDFSDataStore.xml contains some properties.

Assuming all HDFS files correspond directly to a Hive table, and we’re using a configured Hive metastore, these parameters should be ignored when HDFS Datastore is being used with SparkSQL Connector.

Preparing the environment to run the Stratio Connector-SparkSQL
---------------------------------------------------------------

There are some points that must be taken into acount in order to run the connector properly.

In the directory stratio-connector-sparkSQL/connector-sparkSQL/src/main/config/ you will find a file called connector-application.conf

In this file you need to set:

1) The Spark Master. In the variable "spark.master" you can choose if you want to run Spark in your local (e.g. spark.master = local[4]) or in a cluster (e.g. spark.master = "spark://...").

2) The dependencies. In the variable "spark.jars" you must write the path to some jars located in your local after compiling the connector. These jars are:

   Jars needed for the Stratio Connector-SparkSQL:

       - stratio-connector-sparksql-[connector_version].jar
       - crossdata-common-[crossdata_version].jar
       - stratio-connector-commons-[connector_commons_version].jar
       - spark-hive_2.10-1.3.1.jar
       - guava-14.0.1.jar

   Jars needed for the `Spark-Cassandra Provider <https://github.com/Stratio/spark-cassandra-connector>`__

       - cassandra-driver-core-2.1.5.jar
       - cassandra-thrift-2.1.3.jar
       - mysql-connector-java-5.1.34.jar
       - spark-cassandra-connector[spark-cassandra_provider_version].jar

3) Memory dedicated to the driver and the executor as well as the number of cores required. In the variables "spark.driver.memory","spark.executor.memory" and "spark.cores.max" you can set these properties.

4) The Spark home. In the variable "spark.home" you need to write the path to the directory where spark is installed.

.. warning::

You must add core-site.xml and hdfs-site.xml into the config folder (src/main/config) if you require high availability. If these folders are added in the config folder, the connector will take this configuration by default.

In the cluster the following services must be installed and running:

1) HDFS (version 2.4.1 or higher)

2) MySQL (version 14.14 or higher)

3) Spark (version 1.3.0 or higher)

4) Cassandra (version 2.0.0 or higher)


Running the Stratio Connector-SparkSQL
--------------------------------------

To run Stratio Connector-SparkSQL, in the directory

::

       > stratio-connector-sparkSQL/connector-sparkSQL/

Edit the file target/stratio-connector-hdfs-[version]/bin/stratio-connector-hdfs-[version] and write your user in the variables:

::

  > serviceUser="root"

::

  > serviceGroup="root"

After that, execute

::

    > target/stratio-connector-hdfs-[version]/bin/stratio-connector-hdfs-[version] start


Build a redistributable package
-------------------------------

It is possible too, to create a RPM or DEB redistributable package.

RPM Package:

    > mvn unix:package-rpm -N

DEB Package:

    > mvn unix:package-deb -N

Once the package it’s created, execute this commands to install:

RPM Package:

    > rpm -i target/stratio-connector-sparksql-[version].rpm

DEB Package:

    > dpkg -i target/stratio-connector-sparksql-[version].deb

Now to start/stop the connector:

    > service stratio-connector-sparksql start
    > service stratio-connector-sparksql stop

How to use the Stratio Connector-SparkSQL
-----------------------------------------

A complete tutorial is available `here <https://github.com/Stratio/stratio-connector-sparkSQL/blob/master/doc/src/site/sphinx/First_Steps.rst>`__. The basic commands are described below.

1.  Start `Stratio Crossdata Server and then Stratio Crossdata Shell <https://github.com/Stratio/crossdata>`__.

2.  Start the Stratio Connector-SparkSQL as explained before.

3.  In the Stratio Crossdata Shell:

    Add a datastore with this command. We need to specified the XML manifest that defines the data store. The XML manifest can be found in the path of the HDFS Connector in target/stratio-connector-sparksql-[version]/conf/HDFSDataStore.xml

        xdsh:user>  ADD DATASTORE "<Absolute path to HDFS Datastore manifest>";

    Attach cluster on that datastore. The datastore name must be the same as the defined in the Datastore manifest. Remember that defined options at this manifest will be ignored as Stratio Connector-SparkSQL doesn’t need them.

        xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'hosts': '', 'path': '/path', 'highavailability' : ''};

    Add the connector manifest. The XML with the manifest can be found in the path of the Stratio Connector-SparkSQL in target/stratio-connector-sparksql-[version]/conf/SparkSQLConnector.xml

        xdsh:user>  ADD CONNECTOR "<Path to Stratio Connector-SparkSQL Manifest>";

    Attach the connector to the previously defined cluster. The connector name must match the one defined in the Connector Manifest.

        xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit':<LimitSize>};

At this point, we can start to send queries in the Stratio Crossdata Shell.

License
=======

Stratio Connector-SparkSQL is licensed as
`Apache2 <http://www.apache.org/licenses/LICENSE-2.0.txt>`__

Licensed to STRATIO (C) under one or more contributor license
agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The STRATIO (C)
licenses this file to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
