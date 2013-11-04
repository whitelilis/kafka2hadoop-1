package kafka.etl;

import java.io.*;

import org.apache.hadoop.mapred.*;

public final class KafkaETLInputSplit implements InputSplit {

	private static final String[] EMPTY_STRING_ARRAY = new String[] {};
	private static final int CURRENT_VERSION = 1;
	
	private KafkaETLRequest request;
	private long offset;
	private long length;
	private boolean persistOffsetsEnabled;

	public KafkaETLInputSplit() {
	}

	public KafkaETLInputSplit(KafkaETLRequest request, long offset, long length) {
		this.request = request;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public String[] getLocations() throws IOException {
		return EMPTY_STRING_ARRAY;
	}
	
	public long getOffset() {
		return offset;
	}
	
	public String getHost() {
		return request.getURI().getHost();
	}
	
	public int getPort() {
		return request.getURI().getPort();
	}
	
	public String getTopic() {
		return request.getTopic();
	}
	
	public int getPartition() {
		return request.getPartition();
	}

	@Override
	public long getLength() {
		return length;
	}
	
	public boolean isPersistOffsetsEnabled() {
		return persistOffsetsEnabled;
	}
	
	public String serializeRequest(long forOffset) {
		return request.toString(forOffset);
	}
	
	@Override
	public String toString() {
		return "KafkaETLInputSplit [request='" + request + "', offset=" + offset + ", length=" + length + "]";
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		in.readInt(); // ignoring version as we're still on the first
		request = new KafkaETLRequest(in.readUTF());
		offset = in.readLong();
		length = in.readLong();
		persistOffsetsEnabled = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(CURRENT_VERSION);
		out.writeUTF(request.toString());
		out.writeLong(offset);
		out.writeLong(length);
		out.writeBoolean(persistOffsetsEnabled);
	}

	public void setPersistOffsetsEnabled(boolean persistOffsets) {
		this.persistOffsetsEnabled = persistOffsets;
	}

}