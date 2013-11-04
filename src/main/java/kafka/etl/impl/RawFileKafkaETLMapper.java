/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.etl.impl;

import static kafka.etl.KafkaETLRecordReader.*;

import java.io.*;
import java.net.*;

import kafka.etl.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

/**
 * Simple implementation of KafkaETLMapper. It assumes that 
 * input data are text timestamp (long).
 */
public class RawFileKafkaETLMapper implements
Mapper<LongWritable, Text, LongWritable, Text> {
	
	private ObjectMapper jsonMapper = new ObjectMapper();
	private double earliestTimestamp = Double.MAX_VALUE;
	private double latestTimestamp = 0;
	private MultipleOutputs mos;
	private boolean persistOffsetsEnabled;
	private long read = 0;
	private KafkaETLInputSplit split;
	private Props props;
	private String attemptId;
	private long initialOffset;
	private long nextOffset;

    @Override
    public void map(LongWritable key, Text val,
            OutputCollector<LongWritable, Text> collector,
            Reporter reporter) throws IOException {
        
		byte[] bytes = val.getBytes();
        read += bytes.length;
		collector.collect(null, new Text(updateJson(bytes)));
    }


    @Override
    public void configure(JobConf conf) {
    	
    	initAttempId(conf);
    	
    	props = KafkaETLUtils.getPropsFromJob(conf);
    	
    	String input = conf.get("map.input.file");
		System.out.println("input file " + input);
    	persistOffsetsEnabled = conf.getBoolean("map.persist.offset.enabled", false);
	    mos = new MultipleOutputs(conf);
	    
	    initialOffset = KafkaETLRawFileInputFormat.getOffset(input);
	    System.out.println("initial offset: "+ initialOffset);
	    try {
			KafkaETLRequest request = new KafkaETLRequest(conf.get("map.input.topic"), conf.get("map.input.url"), conf.getInt("map.input.partition", 0));
			split = new KafkaETLInputSplit(request, initialOffset, Long.parseLong(conf.get("map.input.file.size")));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	    
	    nextOffset = conf.getLong("kafka.etl.next.offset."+conf.get("map.input.url"), 0);
	    
	    System.out.println("nextOffset: " +nextOffset);
    }

	private void initAttempId(JobConf conf) {
		String taskId = conf.get("mapred.task.id");
		if (taskId == null) {
			throw new IllegalArgumentException(
					"Configutaion does not contain the property mapred.task.id");
		}
		
		String[] parts = taskId.split("_");
		if (    parts.length != 6 || !parts[0].equals("attempt") 
				|| (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
			throw new IllegalArgumentException(
					"TaskAttemptId string : " + taskId + " is not properly formed");
		}
		attemptId = parts[4]+parts[3];
		System.out.println("attempId: " + attemptId);
	}


    @Override
    public void close() throws IOException {

		if (persistOffsetsEnabled) {
			String offsetString = split.serializeRequest(nextOffset);
			
			@SuppressWarnings("unchecked")
			OutputCollector<KafkaETLKey, BytesWritable> offsetOut = (OutputCollector<KafkaETLKey, BytesWritable>) mos.getCollector("offsets", attemptId, Reporter.NULL);
			offsetOut.collect(DUMMY_KEY, new BytesWritable(offsetString.getBytes("UTF-8")));
		}

		@SuppressWarnings("unchecked")
		OutputCollector<NullWritable, Text> infoOut = (OutputCollector<NullWritable, Text>) mos.getCollector("info", attemptId, Reporter.NULL);
		infoOut.collect(null, new Text(generateMeta()));

		mos.close();

    }
    
	private byte[] updateJson(byte[] bytes) throws JsonParseException, IOException {
		
		JsonNode root = jsonMapper.readTree(bytes);
		
		double ts = root.get("timestamp").asDouble();
					
		earliestTimestamp = Math.min(earliestTimestamp, ts);
		latestTimestamp = Math.max(latestTimestamp, ts);
		
		if (root.has("p_ctr") && root.get("p_ctr").isBoolean()) {
			((ObjectNode)root).set("p_ctr", ZERO_DOUBLE_NODE);
		}
		
		return root.toString().getBytes(utfCharset);
	
	}
	
    private String generateMeta() throws IOException {
    	
    	ObjectNode info = jsonMapper.createObjectNode();
    	
    	info.set("earliestTimestamp", DoubleNode.valueOf(earliestTimestamp));
    	info.set("latestTimestamp", DoubleNode.valueOf(latestTimestamp));
    	info.set("host", TextNode.valueOf(split.getHost()));
    	info.set("port", IntNode.valueOf(split.getPort()));
    	info.set("topic", TextNode.valueOf(split.getTopic()));
    	info.set("partition", IntNode.valueOf(split.getPartition()));
    	info.set("offset", LongNode.valueOf(initialOffset));
    	info.set("length", LongNode.valueOf(read));
    	
    	return info.toString();
    }



}
