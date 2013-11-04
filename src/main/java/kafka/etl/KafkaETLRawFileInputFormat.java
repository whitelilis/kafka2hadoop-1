package kafka.etl;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class KafkaETLRawFileInputFormat extends TextInputFormat {

	static class InternalFileSplit extends FileSplit {

		private boolean persistOffsetEnabled;
		private String url;
		private String topic;
		private int partition;

		public InternalFileSplit() {
			super(null, 0, 0, (String[]) null);
		}

		public InternalFileSplit(FileSplit split) throws IOException {
			super(split.getPath(), split.getStart(), split.getLength(), split.getLocations());
		}

		public InternalFileSplit(Path file, long start, long length, String[] hosts, String url, String topic, int partition) {
			super(file, start, length, hosts);
			this.url = url;
			this.topic = topic;
			this.partition = partition;
		}

		public void setPersistOffsetEnabled(boolean persistOffsetEnabled) {
			this.persistOffsetEnabled = persistOffsetEnabled;
		}

		public boolean isPersistOffsetEnabled() {
			return persistOffsetEnabled;
		}
		
		public String getUrl() {
			return url;
		}
		
		public String getTopic() {
			return topic;
		}
		
		public int getPartition() {
			return partition;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeBoolean(persistOffsetEnabled);
			out.writeUTF(url);
			out.writeUTF(topic);
			out.writeInt(partition);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			persistOffsetEnabled = in.readBoolean();
			url = in.readUTF();
			topic = in.readUTF();
			partition = in.readInt();
		}

	}

	private int max_files;

	private List<InternalFileSplit> splits(JobConf conf) throws IOException {

		List<InternalFileSplit> splits = new ArrayList<InternalFileSplit>();
		
		Props props = KafkaETLUtils.getPropsFromJob(conf);
		
		try {
			max_files = props.getInt("kafka.etl.files.per.request", 150);
		} catch (Exception e) {
			max_files = 150;
		}
		
		List<KafkaETLRequest> requests = KafkaETLInputFormat.readRequestsFromInputs(conf, KafkaETLUtils.listStatus(conf, KafkaETLUtils.getInputPaths(props.getProperty("input"))));
		Map<String, KafkaETLRequest> map = new HashMap<String, KafkaETLRequest>();
		for (KafkaETLRequest r : requests) {
			map.put(r.getURI().getHost(), r);
		}
		
		KafkaETLRequest kafka1 = map.get("kafka1.madvertise.net");
		KafkaETLRequest kafka2 = map.get("kafka2.madvertise.net");
		
		splits.addAll(splits(conf, "/kafka.export.1/*.kafka.gz", "tcp://kafka1.madvertise.net:9092", "events", 0, kafka1 == null? 0 : kafka1.getOffset()));
		splits.addAll(splits(conf, "/kafka.export.2/*.kafka.gz", "tcp://kafka2.madvertise.net:9092", "events", 0, kafka2 == null? 0 : kafka2.getOffset()));

		return splits;
	}

	private List<InternalFileSplit> splits(JobConf conf, String input, String url, String topic, int partition, long offset) throws IOException, InvalidInputException {

		List<FileStatus> files = KafkaETLUtils.listStatus(conf, KafkaETLUtils.getInputPaths(input));

		System.out.println(files.size() + " files in input " + input);
		
		System.out.println("Start searching from index: " + offset);

		Collections.sort(files, new Comparator<FileStatus>() {
			@Override
			public int compare(FileStatus f1, FileStatus f2) {
				return f1.getPath().toString().compareTo(f2.getPath().toString());
			}
		});

		Iterator<FileStatus> it = files.iterator();
		int skipping = 0;
		while(it.hasNext()) {
			FileStatus f = it.next();
			if (getOffset(f.getPath().toString()) >= offset) {
				break;
			}
			it.remove();
			skipping++;
		}
		
		System.out.println("Skipping "+ skipping + " files");

		int nextBatchOffsetIndex = Math.min(max_files+1, files.size())-1;
		
		System.out.println("nextBatchStart: " + nextBatchOffsetIndex);
		
		List<FileStatus> set = files.subList(0, nextBatchOffsetIndex);

		List<InternalFileSplit> splits = new ArrayList<InternalFileSplit>();
		for (FileStatus f : set) {
			splits.add(new InternalFileSplit(f.getPath(), 0, f.getLen(), null, url, topic, partition));
		}

		if (!splits.isEmpty()) {
			splits.get(splits.size() - 1).setPersistOffsetEnabled(true);
		}

		long nextBatchOffset = getOffset(files.get(nextBatchOffsetIndex).getPath().toString());
		conf.setLong("kafka.etl.next.offset."+url, nextBatchOffset);
		System.out.println("nextBatchStart offset: " + nextBatchOffset);
		return splits;
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int hint) throws IOException {
		List<InternalFileSplit> splits = splits(conf);
		if (splits.isEmpty()) {
			throw new IOException("No more files to process");
		}
		System.out.println("Reading: " + splits.size() + " files");
		return splits.toArray(new InputSplit[splits.size()]);
	}

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {

		InternalFileSplit split = (InternalFileSplit) genericSplit;

		job.set("map.input.file", split.getPath().toString());
		job.setLong("map.input.file.size", split.getLength());
		job.setBoolean("map.persist.offset.enabled", split.isPersistOffsetEnabled());
		job.set("map.input.url", split.getUrl());
		job.set("map.input.topic", split.getTopic());
		job.setInt("map.input.partition", split.getPartition());

		return super.getRecordReader(genericSplit, job, reporter);
	}

	public static long getOffset(String input) {
		int t = input.lastIndexOf('/');
		return Long.parseLong(input.substring(t + 1, input.indexOf('.', t)));
	}

}
