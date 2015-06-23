Metastore configuration
***********************

MySQL Configuration
==================

Install MySQL
-------------

Initially we need to install MySQL in one of the nodes, necessary for storing Spark metadata in a JDBC database.
You can use another database if it provides JDBC connection.

Step 1: Installing the server and the client
--------------------------------------------

To install it, execute:
::

      > apt-get install mysql-server mysql-client

Step 2: Creating the user
-------------------------
Spark SQL needs to create the metadata, so we need to create an user with credentials for that.

::

      > CREATE USER 'stratio'@'%' IDENTIFIED BY 'stratio';

::

      > REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'stratio'@'%';

::

      > GRANT SELECT,INSERT,UPDATE,DELETE,LOCK TABLES,EXECUTE,CREATE,INDEX,ALTER,DROP ON testmetastore.* TO 'stratio'@'%';

::

      > FLUSH PRIVILEGES;

::

      > quit;

Step 3: Starting the service
----------------------------

Finally for initializing all, it is necessary to execute the following command:

::

      > /sbin/service mysqld start

Step 4: Setting Spark files
---------------------------------

In ${SPARK_HOME}/conf we need to modify the path of the database, the user and the password as we did with the parameters in the step 2.

hive-site.xml
.............

<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

    <!-- Hive Configuration can either be stored in this file or in the hadoop configuration files  -->
    <!-- that are implied by Hadoop setup variables.                                                -->
    <!-- Aside from Hadoop setup variables - this file is provided as a convenience so that Hive    -->
    <!-- users do not have to edit hadoop configuration files (that may be managed as a centralized -->
    <!-- resource).                                                                                 -->

    <!-- Hive Execution Parameters -->

    <property>
       <name>javax.jdo.option.ConnectionURL</name>
       <value>jdbc:mysql://aws01/testmetastore?createDatabaseIfNotExist=true</value>
       <description>JDBC connect string for a JDBC metastore</description>
    </property>

    <property>
       <name>javax.jdo.option.ConnectionDriverName</name>
       <value>com.mysql.jdbc.Driver</value>
       <description>Driver class name for a JDBC metastore</description>
    </property>

    <property>
       <name>javax.jdo.option.ConnectionUserName</name>
       <value>stratio</value>
    </property>

    <property>
       <name>javax.jdo.option.ConnectionPassword</name>
       <value>stratio</value>
    </property>

    <property>
       <name>datanucleus.autoCreateSchema</name>
       <value>true</value>
    </property>
</configuration>