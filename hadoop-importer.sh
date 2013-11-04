#!/bin/bash

# Run in UTC
export TZ="/usr/share/zoneinfo/UTC"

#script uses relative paths, fixing it the old fashion way ;)
cd `dirname $0`

if [ -z "$topic" ]; then
   echo "***************************************************************************************************************************"
   echo "Must set $topic to kafka topic"
   echo "***************************************************************************************************************************"
   exit 1
fi

if [ -z "$hdfs_dir" ]; then
   echo "***************************************************************************************************************************"
   echo "Must set $hdfs_dir to HDFS path"
   echo "***************************************************************************************************************************"
   exit 1
fi

if [ -z "$bucket_name" ]; then
   bucket_name=`date +%Y/%m/%d/%Hh%M/`
fi

if [ -z "$generated_property_file" ]; then
   echo "***************************************************************************************************************************"
   echo "Must set $generated_property_file to filename we can use for storing state"
   echo "***************************************************************************************************************************"
   exit 1
fi

hdfs_input="`hadoop fs -ls ${hdfs_dir}/*/*/*/*/_SUCCESS | sort -k 8 | tail -1 | awk '{printf $8'} | sed -e 's/_SUCCESS/offsets_*/'`"

if [ -z "$hdfs_input" ]; then
   hdfs_input=${hdfs_dir}/offset
fi

current_offset_file_exists=`hadoop fs -ls ${hdfs_input}`

eval "echo \"$(< template.properties)\"" > ${generated_property_file}

if [ -z "$current_offset_file_exists" ]; then
   echo "***************************************************************************************************************************"
   echo "WARNING: Offset file(s) not found. The hadoop job cannot be run."
   echo "To generate the initial offset files, please run the following command:"
   echo "./initalize-hadoop.sh"
   echo "***************************************************************************************************************************"

   exit 1
else
   echo "***************************************************************************************************************************"
   echo "Importing topic '${topic}' to HDFS directory: ${hdfs_dir}/${bucket_name}"
   echo "***************************************************************************************************************************"

   ./run-class.sh kafka.etl.impl.SimpleKafkaETLJob ${generated_property_file}
fi

