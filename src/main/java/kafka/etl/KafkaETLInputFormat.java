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
package kafka.etl;

import static kafka.etl.KafkaETLUtils.*;

import java.io.*;
import java.util.*;

import kafka.api.*;
import kafka.consumer.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

public class KafkaETLInputFormat implements InputFormat<KafkaETLKey, BytesWritable> {

	private static final int DEFAULT_MAX_KAFKA_LOGS_PER_MAPPER = 6;
	private static final int DEFAULT_MAX_MAPPERS_PER_REQUEST = 150;
	private static final int MAX_OFFSETS = 50000;

	private Props props;

	private int maxKafkaLogFilesPerMapper;

	private int maxMappersPerRequest;
	private MultipleOutputs mos;

	@Override
	public RecordReader<KafkaETLKey, BytesWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter) throws IOException {
		return new KafkaETLRecordReader((KafkaETLInputSplit) split, conf, reporter);
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int hint) throws IOException {
		
		this.mos = new MultipleOutputs(conf);
		readProps(conf);
		
		List<KafkaETLRequest> requests = readRequestsFromInputs(conf);
		
		try {
			return getSplitsFromRequests(requests);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void readProps(JobConf conf) {
		props = KafkaETLUtils.getPropsFromJob(conf);
		
		try {
			maxKafkaLogFilesPerMapper = props.getInt("kafka.etl.kafka.maxKafkaLogsPerMapper", DEFAULT_MAX_KAFKA_LOGS_PER_MAPPER);
			maxMappersPerRequest = props.getInt("kafka.etl.kafka.maxMappersPerRequest", DEFAULT_MAX_MAPPERS_PER_REQUEST);
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}

	private InputSplit[] getSplitsFromRequests(List<KafkaETLRequest> requests) throws IOException, Exception {

		ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

		for (KafkaETLRequest request : requests) {
			
			long[] offsets = getOffsets(request);
	
			long earliest = request.getOffset();
			
			System.out.println("Earliest to be fetched: "+ earliest);
			
			System.out.println("Data remaining on the stream: "+ humanReadableByteCount(offsets[0]-earliest) + " on topic " + request.getTopic()+":"+request.getPartition());
			
			ArrayList<KafkaETLInputSplit> requestSplits = getSplits(request, offsets, earliest);
			
			if (requestSplits.isEmpty()) {
				System.out.println("Nothing to do for request: " +request);
				dumpLastOffsetToNewJobDirectory(request);
				continue;
			}
			
			KafkaETLInputSplit last = requestSplits.get(requestSplits.size()-1);
			last.setPersistOffsetsEnabled(true);
			
			System.out.println("Deviding the remaining " + humanReadableByteCount(last.getOffset()+last.getLength()-earliest) + " in " + requestSplits.size() + " map tasks");
			
			splits.addAll(requestSplits);
		}
		

		return splits.toArray(new InputSplit[splits.size()]);
	}

	private void dumpLastOffsetToNewJobDirectory(KafkaETLRequest request) throws IOException, UnsupportedEncodingException {
		@SuppressWarnings("unchecked")
		OutputCollector<KafkaETLKey, BytesWritable> offsetOut = (OutputCollector<KafkaETLKey, BytesWritable>) mos.getCollector("offsets", request.getUniqueID(), Reporter.NULL);
		offsetOut.collect(kafka.etl.KafkaETLRecordReader.DUMMY_KEY, new BytesWritable(request.toString().getBytes("UTF-8")));
	}

	private ArrayList<KafkaETLInputSplit> getSplits(KafkaETLRequest request, long[] offsets, long earliest) {
		ArrayList<KafkaETLInputSplit> requestSplits = new ArrayList<KafkaETLInputSplit>();

		int count = 0;
		int dec = 1;
		for (int i = offsets.length - 1; i > 0; i -= dec) {
			long offset = offsets[i];

			if (offset < earliest)
				continue;

			if (++count == maxMappersPerRequest)
				break;

			if (requestSplits.isEmpty()) {
				offset = earliest;
				dec = maxKafkaLogFilesPerMapper;
			}

			dec = Math.min(maxKafkaLogFilesPerMapper, i);

			addSplit(request, requestSplits, offset, offsets[i - dec] - offset);

		}
		return requestSplits;
	}

	private long[] getOffsets(KafkaETLRequest request) throws Exception {
		System.out.println("Fetching offsets from " + request.getURI().getHost()+":"+request.getURI().getPort());

		SimpleConsumer consumer = new SimpleConsumer(request.getURI().getHost(), request.getURI().getPort(), getClientTimeout(props), getClientBufferSize(props));

		long[] offsets = consumer.getOffsetsBefore(request.getTopic(), request.getPartition(), OffsetRequest.LatestTime(), MAX_OFFSETS);
		
		System.out.println(offsets.length + " offsets received");
		
		consumer.close();
		return offsets;
	}

	public static List<KafkaETLRequest> readRequestsFromInputs(JobConf conf) throws IOException, UnsupportedEncodingException {
		List<FileStatus> files = KafkaETLUtils.listStatus(conf);
		return readRequestsFromInputs(conf, files);
	}

	public static List<KafkaETLRequest> readRequestsFromInputs(JobConf conf, List<FileStatus> files) throws IOException, UnsupportedEncodingException {
		List<KafkaETLRequest> requests = new ArrayList<KafkaETLRequest>();
		for (FileStatus file : files) {
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(conf);
			Reader in = new SequenceFile.Reader(fs, path, conf);
			
			try {
				KafkaETLKey key = new KafkaETLKey();
				if (!in.next(key)) {
					throw new IOException("File " + path + " is corrupted, no value found for key " + key);
				}
				
				BytesWritable value = new BytesWritable();
				in.getCurrentValue(value);
				
	            String input = new String(value.getBytes(), "UTF-8");
	            KafkaETLRequest request = new KafkaETLRequest(input.trim());
	
	            System.out.println("Input " + path + ": " + request);
	            
	            requests.add(request);
	            
			} finally {
				in.close();
			}
		}
		return requests;
	}

	private KafkaETLInputSplit addSplit(KafkaETLRequest source, ArrayList<KafkaETLInputSplit> splits, long off, long len) {
		KafkaETLInputSplit split = new KafkaETLInputSplit(source, off, len);
		splits.add(split);
		return split;
	}

}
