package kafka.etl;

import static kafka.etl.KafkaETLUtils.*;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import kafka.api.*;
import kafka.common.*;
import kafka.javaapi.MultiFetchResponse;
import kafka.javaapi.consumer.*;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;

public class KafkaETLRecordReader implements RecordReader<KafkaETLKey, BytesWritable> {
	
	public final static DoubleNode ZERO_DOUBLE_NODE = new DoubleNode(0);
	public final static KafkaETLKey DUMMY_KEY = new KafkaETLKey();
	private final static int MAX_RETRY_TIME = 1;
	public static final Charset utfCharset = Charset.forName("UTF-8");

	private SimpleConsumer consumer;
	private Props props;
	private KafkaETLInputSplit split;
	private Iterator<MessageAndOffset> messageIt;
	private MultiFetchResponse response;
	private Iterator<ByteBufferMessageSet> respIterator;
	private long requestTime;
	private long offset;
	private long count;
	private int index;
	private int bufferSize;
	private int retry;
	private MultipleOutputs mos;
	private String attemptId;
	private Reporter reporter;
	private long startTime;
	private double earliestTimestamp = Double.MAX_VALUE;
	private double latestTimestamp = 0;
	private ObjectMapper jsonMapper;
	

	public KafkaETLRecordReader(KafkaETLInputSplit split, JobConf conf, Reporter reporter) {
		this.split = split;
		this.reporter = reporter;
		System.out.println("Creating kafka constumer for: " + split);
		
		props = getPropsFromJob(conf);
		
		initAttempId(conf);
		
		try {
			bufferSize = getClientBufferSize(props);
			consumer = new SimpleConsumer(split.getHost(), split.getPort(), getClientTimeout(props), bufferSize);
			offset = split.getOffset();
			fetchMore();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	    mos = new MultipleOutputs(conf);
	    
        startTime = System.currentTimeMillis();
        
        jsonMapper = new ObjectMapper();
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

    public boolean hasMore () {
        return messageIt != null && messageIt.hasNext() 
                || response != null && respIterator.hasNext()
                || offset < split.getOffset()+split.getLength(); 
    }
    
    public boolean getNext(KafkaETLKey key, BytesWritable value) throws IOException {
        if ( !hasMore() ) return false;
        
        boolean gotNext = get(key, value);
        
        if(response != null) {

            while ( !gotNext && respIterator.hasNext()) {
                ByteBufferMessageSet msgSet = respIterator.next();
                if ( hasError(msgSet)) return false;
                messageIt = msgSet.iterator();
                gotNext = get(key, value);
            }
        }
        return gotNext;
    }
    
    public boolean fetchMore () throws IOException {
        if (!hasMore()) return false;
        
        FetchRequest fetchRequest = 
            new FetchRequest(split.getTopic(), split.getPartition(), offset, bufferSize);
        List<FetchRequest> array = new ArrayList<FetchRequest>();
        array.add(fetchRequest);

        long tempTime = System.currentTimeMillis();
        response = consumer.multifetch(array);
        if(response != null)
            respIterator = response.iterator();
        requestTime += (System.currentTimeMillis() - tempTime);
        
        return true;
    }
    
    @Override
	public void close() throws IOException {

		if (consumer != null) {
			consumer.close();
		}

		if (split.isPersistOffsetsEnabled()) {
			String offsetString = split.serializeRequest(offset);

			@SuppressWarnings("unchecked")
			OutputCollector<KafkaETLKey, BytesWritable> offsetOut = (OutputCollector<KafkaETLKey, BytesWritable>) mos.getCollector("offsets", attemptId, reporter);
			offsetOut.collect(DUMMY_KEY, new BytesWritable(offsetString.getBytes("UTF-8")));
		}

		@SuppressWarnings("unchecked")
		OutputCollector<NullWritable, Text> infoOut = (OutputCollector<NullWritable, Text>) mos.getCollector("info", attemptId, reporter);
		infoOut.collect(null, new Text(generateMeta()));

		mos.close();

		String topic = split.getTopic();
		long endTime = System.currentTimeMillis();
		reporter.incrCounter(topic, "read-time(ms)", endTime - startTime);
		reporter.incrCounter(topic, "request-time(ms)", requestTime);

		long bytesRead = offset - split.getOffset();
		reporter.incrCounter(topic, "data-read", bytesRead);
		reporter.incrCounter(topic, "event-count", count);
	}
    
    private String generateMeta() throws IOException {
    	
    	ObjectNode info = jsonMapper.createObjectNode();
    	
    	info.set("earliestTimestamp", DoubleNode.valueOf(earliestTimestamp));
    	info.set("latestTimestamp", DoubleNode.valueOf(latestTimestamp));
    	info.set("host", TextNode.valueOf(split.getHost()));
    	info.set("port", IntNode.valueOf(split.getPort()));
    	info.set("topic", TextNode.valueOf(split.getTopic()));
    	info.set("partition", IntNode.valueOf(split.getPartition()));
    	info.set("offset", LongNode.valueOf(split.getOffset()));
    	info.set("length", LongNode.valueOf(split.getLength()));
    	info.set("taskId", TextNode.valueOf(attemptId));
    	
    	return info.toString();
    }
    
    
	protected boolean get(KafkaETLKey key, BytesWritable value) throws IOException {
		if (messageIt == null || !messageIt.hasNext()) {
			return false;
		}

		MessageAndOffset messageAndOffset = messageIt.next();

		ByteBuffer buf = messageAndOffset.message().payload();
		int origSize = buf.remaining();
		byte[] bytes = new byte[origSize];
		buf.get(bytes, buf.position(), origSize);
		
		bytes = updateJson(bytes);
		
		value.set(bytes, 0, bytes.length);

		key.set(index, offset, 0);

		offset = messageAndOffset.offset();
		count++;


		return true;
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

	/**
     * Called by the default implementation of {@link #map} to check error code
     * to determine whether to continue.
     */
    protected boolean hasError(ByteBufferMessageSet messages)
            throws IOException {
        int errorCode = messages.getErrorCode();
        if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
            /* offset cannot cross the maximum offset (guaranteed by Kafka protocol).
               Kafka server may delete old files from time to time */
            System.err.println("WARNING: current offset=" + offset + ". It is out of range.");

            if (retry >= MAX_RETRY_TIME)  return true;
            retry++;
            // get the current offset range
            offset =  split.getOffset();
            return false;
        } else if (errorCode == ErrorMapping.InvalidMessageCode()) {
            throw new IOException(split + " current offset=" + offset
                    + " : invalid offset.");
        } else if (errorCode == ErrorMapping.WrongPartitionCode()) {
            throw new IOException(split + " : wrong partition");
        } else if (errorCode != ErrorMapping.NoError()) {
            throw new IOException(split + " current offset=" + offset
                    + " error:" + errorCode);
        } else
            return false;
    }

	@Override
	public KafkaETLKey createKey() {
		return new KafkaETLKey();
	}

	@Override
	public BytesWritable createValue() {
		return new BytesWritable();
	}

	@Override
	public long getPos() throws IOException {
		return offset;
	}

	@Override
	public float getProgress() {
		return (offset-split.getOffset())/(float)split.getLength();
	}

	@Override
	public boolean next(KafkaETLKey key, BytesWritable value) throws IOException {
		if (!getNext(key, value)) {
			fetchMore();
			return getNext(key, value);
		}
		return true;
	}
	
}