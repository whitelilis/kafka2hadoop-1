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

if [ -z "$list_of_brokers" ]; then
   echo "***************************************************************************************************************************"
   echo "Must set $list_of_brokers to a config file containing list of kafka servers"
   echo "***************************************************************************************************************************"
   exit 1
fi

if [ -z "$generated_property_file" ]; then
   echo "***************************************************************************************************************************"
   echo "Must set $generated_property_file to filename we can use for storing state"
   echo "***************************************************************************************************************************"
   exit 1
fi

if [ -z "$bucket_name" ]; then
   bucket_name=`date +%Y/%m/%d/%Hh%M/`
fi

current_offset_file_exists=`hadoop fs -ls ${hdfs_dir}/${bucket_name}/*.dat`

if [ -z "$current_offset_file_exists" ]; then
   echo "***************************************************************************************************************************"
   echo "No offset file(s) found, so new one(s) will be generated for the topic '${topic}' starting from offset -1"
   echo "***************************************************************************************************************************"

   if [ -f $list_of_brokers ]
   then
      printf "File \"$list_of_brokers\" was found\n"
      while read server
      do
         broker=${server}
         offset_file_name=`echo ${server} | sed -e 's/\:/-port/g'`
         hdfs_input=${hdfs_dir}/${bucket_name}
         eval "echo \"$(< template.properties)\"" > ${generated_property_file}
         ./run-class.sh kafka.etl.impl.DataGenerator ${generated_property_file}
         hadoop fs -mv ${hdfs_dir}/${bucket_name}/1.dat ${hdfs_dir}/${bucket_name}/${offset_file_name}.dat
      done < $list_of_brokers
   else
      printf "File \"$list_of_brokers\" was NOT found\n"
      exit 0
   fi
   hadoop fs -touchz  ${hdfs_dir}/${bucket_name}/_SUCCESS

else
   echo "***************************************************************************************************************************"
   echo "Offset file(s) already found in ${hdfs_dir}/offset so no new one(s) will be created."
   echo "***************************************************************************************************************************"
fi
