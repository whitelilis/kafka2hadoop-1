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

