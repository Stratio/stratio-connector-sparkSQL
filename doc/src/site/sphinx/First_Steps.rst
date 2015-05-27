First Steps
***********

Table of Contents
=================

-  `Before you start <#before-you-start>`__

   -  `Prerequisites <#prerequisites>`__
   -  `Configuration <#configuration>`__

-  `Registering the catalog and the
   collection <#registering-the-catalog-and-the-collection>`__

   -  `Step 1: Creating the catalog <#step-1-creating-the-catalog>`__
   -  `Step 2: Registering the collection <#step-2-registering-the-collection>`__

-  `Querying Data <#querying-data>`__

-  `Where to go from here <#where-to-go-from-here>`__

Before you start
================

Prerequisites
-------------
- You need to install sbt and maven.

- `Stratio Crossdata <https://github.com/Stratio/crossdata>`__ is needed in order to interact with this connector.

- An existing and deployed `Hive metastore <https://hive.apache.org/>`__.

- Build a SparkSQLConnector executable and run it following this `guide <https://github.com/Stratio/stratio-connector-sparkSQL/blob/master/doc/src/site/sphinx/about.rst>`__.

Configuration
-------------

In the Stratio Crossdata Shell we need to add the Datastore Manifest.

::

       > add datastore "<path_to_manifest_folder>/HDFSDataStore.xml";

Now we need to add the ConnectorManifest.

::

       > add connector "<path_to_manifest_folder>/SparkSQLConnector.xml";

At this point we have reported to Stratio Crossdata the connector options and
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

To ensure that the connector is online we can execute the Stratio Crossdata
Shell command:

::

      > describe connectors;

And the output must show a message similar to:

::

    Connector: connector.sparkSQLConnector  ONLINE  []  [datastore.hdfs]    akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

Registering the catalog and the collection
======================================

Step 1: Creating the catalog
----------------------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG metastore;

The output must be:

::

    CATALOG created successfully

Step 2: Registering the collection
----------------------------------

To register the table, remember it has to be registered in our Hive metastore (this will provide Spark SQL
enough info to find out which datasource and some other options are needed to retrieve data).
Having assured that, we must execute the next command.

::

      > REGISTER TABLE metastore.students ON CLUSTER hdfsCluster (id int PRIMARY KEY, name text, age int, enrolled boolean);

In case the table was not previously registered in Hive metastore, we can register it by adding associated Datasource parameters.
::

      > REGISTER TABLE metastore.students ON CLUSTER hdfsCluster (id int PRIMARY KEY, name text, age int, enrolled boolean) WITH {'path' : 'my-table-path'};

And the output must show something like:

::

    TABLE created successfully

Querying Data
=============

All we have to do now is launching our query in the Stratio Crossdata Shell.

::

      >  SELECT * FROM metastore.students;


And after that, the query output will be displayed asynchronously on the Stratio Crossdata Shell.

Where to go from here
=====================

To learn more about Stratio Crossdata, we recommend you to visit the `Stratio Crossdata Reference <https://github.com/Stratio/crossdata/tree/master/_doc/meta-reference.md>`__.
