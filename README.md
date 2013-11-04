Parallel Kafka-Hadoop Consumer
==============================

Dependencies
------------
All the dependencies are managed by grandle, which comes with.


Before Installing
-----------------

Take a look on some variables in template.properties, hadoop-importer.sh, copy-jars.sh


Build
-----

	$ ./gradlew jar


Run
---

	$ topic=<topic> hdfs_dir=<target> generated_property_file=<properties> list_of_brokers=<brokers> ./hadoop-importer.sh

where:

	<topic> 	 	well, topic name
	<target>		root for the events, like /events
	<properties>	properties file
	<brokers>		file with linebreak separated list of servers


Eclipse Development
-------------------

To import this project run:

	$ ./gradlew eclipse

That should download all dependencies and generate the appropriate .classpath and .project file.

So just go to your eclipse File -> Import... -> General/Existing Projects into Workspace, and select this directory as root
