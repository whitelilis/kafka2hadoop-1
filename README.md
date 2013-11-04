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

Build:

	$ ./gradlew jar


Copy to hadoop:

	$ ./copy-jars.sh

Run
---

	topic=<topic> hdfs_dir=<target, ex: /events> generated_property_file=my.properties list_of_brokers=<file with linebreak separated list of servers> ./hadoop-importer.sh


Eclipse Development
-------------------

To import this project run:

	$ ./gradlew eclipse

That should download all dependencies and generate the appropriate .classpath and .project file.

So just go to your eclipse File -> Import... -> General/Existing Projects into Workspace, and select this directory as root
