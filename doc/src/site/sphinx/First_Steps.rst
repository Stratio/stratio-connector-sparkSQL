First Steps
***********

SparkSQL Crossdata connector allows the integration between Crossdata and
SparkSQL. Crossdata provides an easy and common language as well as the
integration with several other databases. More information about
Crossdata can be found at
`Crossdata <https://github.com/Stratio/crossdata>`__

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Registering the catalog and
   collection <#registering-the-catalog-and-collection>`__

   -  `Step 1: Create the database <#step-1-create-the-database>`__
   -  `Step 2: Create the collection <#step-2-create-the-collection>`__

-  `Querying Data <#querying-data>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------

-  Basic knowledge of SQL like language.
-  First of all `Stratio Crossdata
   0.3.0 <https://github.com/Stratio/crossdata>`__ is needed and must be
   installed. The server and the shell must be running.
-  An existing and deployed
   `Hive metastore <https://hive.apache.org/>`__.
-  Build a SparkSQLConnector executable and run it following this
   `guide <https://github.com/Stratio/stratio-connector-sparkSQL#build-an-executable-sparksql-connector>`__.

Configuration
-------------

In the Crossdata Shell we need to add the Datastore Manifest.

::

       > add datastore "<path_to_manifest_folder>/HDFSDataStore.xml";

The output must be:

::

       [INFO|Shell] CrossdataManifest added
        DATASTORE
        Name: hdfs
        Version: 2.4.1
        Required properties:
        Property:
            PropertyName: Hosts
            Description: The list of hosts ips (csv). Example: host1,host2,host3
        Property:
            PropertyName: Port
            Description: The list of ports (csv).
        Property:
            PropertyName: Partitions
            Description: Structure of the HDFS
        Property:
            PropertyName: PartitionName
            Description: Name of the File for the different partitions of the table.
        Property:
            PropertyName: Extension
            Description: Extension of the file in HDFS.
        Property:
            PropertyName: FileSeparator
            Description: Character to split the File lines

Now we need to add the ConnectorManifest.

::

       > add connector "<path_to_manifest_folder>/SparkSQLConnector.xml";

The output must be:

::

       [INFO|Shell] CrossdataManifest added
        CONNECTOR
        ConnectorName: SparkSQLConnector
       DataStores:
        DataStoreName: hdfs
        Version: 0.1.0
        Supported operations:
                                .
                                .
                                .

At this point we have reported to Crossdata the connector options and
operations. Now we configure the datastore cluster.

::

    >  ATTACH CLUSTER hdfsCluster ON DATASTORE hdfs WITH OPTIONS {'hosts': '', 'user': '', 'path': '', 'highavailability' : ''};

The output must be similar to:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

Now we run the connector.

The last step is to attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR hdfsconnector TO hdfsCluster  WITH OPTIONS {'DefaultLimit' : <limitSize>};

The output must be:

::

    CONNECTOR attached successfully

To ensure that the connector is online we can execute the Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.sparkSQLConnector  ONLINE  []  [datastore.hdfs]    akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

Registering the catalog and collection
======================================

Step 1: Create the catalog
--------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG metastore;

The output must be:

::

    CATALOG created successfully;

Step 2: Register the collection
-------------------------------

To register the table, remember it has to be registered in our Hive metastore (this will provide SparkSQL
enough info to find out which datasource and some other options are needed to retrieve data).
Having assured that, we must execute the next command.

::

      > REGISTER TABLE metastore.students ON CLUSTER hdfsCluster (id int PRIMARY KEY, name text, age int, enrolled boolean);

And the output must show something like:

::

    TABLE created successfully

Querying Data
=============

All we have to do now is launching our query in Crossdata shell.

::

      >  SELECT * FROM metastore.students;


And after that, query output will be displayed asynchronously on Crossdata shell.

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend to visit the
`Crossdata
Reference <https://github.com/Stratio/crossdata/tree/master/_doc/meta-reference.md>`__.
