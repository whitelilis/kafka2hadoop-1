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


import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.security.*;
import org.apache.hadoop.util.*;

public class KafkaETLUtils {

	final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;
	final static int DEFAULT_TIMEOUT = 60000; // one minute

	final static String CLIENT_BUFFER_SIZE = "client.buffer.size";
	final static String CLIENT_TIMEOUT = "client.so.timeout";

	public static PathFilter PATH_FILTER = new PathFilter() {
		@Override
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};
	
	public static Path getLastPath(Path path, FileSystem fs) throws IOException {

		FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

		if (statuses.length == 0) {
			return path;
		} else {
			Arrays.sort(statuses);
			return statuses[statuses.length - 1].getPath();
		}
	}

	public static String getFileName(Path path) throws IOException {
		String fullname = path.toUri().toString();
		String[] parts = fullname.split(Path.SEPARATOR);
		if (parts.length < 1)
			throw new IOException("Invalid path " + fullname);
		return parts[parts.length - 1];
	}

	public static List<String> readText(FileSystem fs, String inputFile)
			throws IOException, FileNotFoundException {
		Path path = new Path(inputFile);
		return readText(fs, path);
	}

	public static List<String> readText(FileSystem fs, Path path)
			throws IOException, FileNotFoundException {
		if (!fs.exists(path)) {
			throw new FileNotFoundException("File " + path + " doesn't exist!");
		}
		BufferedReader in = new BufferedReader(new InputStreamReader(
				fs.open(path)));
		List<String> buf = new ArrayList<String>();
		String line = null;

		while ((line = in.readLine()) != null) {
			if (line.trim().length() > 0)
				buf.add(new String(line.trim()));
		}
		in.close();
		return buf;
	}

	public static void writeText(FileSystem fs, Path outPath, String content)
			throws IOException {
		long timestamp = System.currentTimeMillis();
		String localFile = "/tmp/KafkaETL_tmp_" + timestamp;
		PrintWriter writer = new PrintWriter(new FileWriter(localFile));
		writer.println(content);
		writer.close();

		Path src = new Path(localFile);
		fs.moveFromLocalFile(src, outPath);
	}

	public static Props getPropsFromJob(Configuration conf) {
		String propsString = conf.get("kafka.etl.props");
		if (propsString == null)
			throw new UndefinedPropertyException(
					"The required property kafka.etl.props was not found in the Configuration.");
		try {
			ByteArrayInputStream input = new ByteArrayInputStream(
					propsString.getBytes("UTF-8"));
			Properties properties = new Properties();
			properties.load(input);
			return new Props(properties);
		} catch (IOException e) {
			throw new RuntimeException("This is not possible!", e);
		}
	}

	 public static void setPropsInJob(Configuration conf, Props props)
	  {
	    ByteArrayOutputStream output = new ByteArrayOutputStream();
	    try
	    {
	      props.store(output);
	      conf.set("kafka.etl.props", new String(output.toByteArray(), "UTF-8"));
	    }
	    catch (IOException e)
	    {
	      throw new RuntimeException("This is not possible!", e);
	    }
	  }
	 
	public static Props readProps(String file) throws IOException {
		Path path = new Path(file);
		FileSystem fs = path.getFileSystem(new Configuration());
		if (fs.exists(path)) {
			InputStream input = fs.open(path);
			try {
				// wrap it up in another layer so that the user can override
				// properties
				Props p = new Props(input);
				return new Props(p);
			} finally {
				input.close();
			}
		} else {
			return new Props();
		}
	}

	public static String findContainingJar(
			@SuppressWarnings("rawtypes") Class my_class, ClassLoader loader) {
		String class_file = my_class.getName().replaceAll("\\.", "/")
				+ ".class";
		return findContainingJar(class_file, loader);
	}

	public static String findContainingJar(String fileName, ClassLoader loader) {
		try {
			for (@SuppressWarnings("rawtypes")
			Enumeration itr = loader.getResources(fileName); itr
					.hasMoreElements();) {
				URL url = (URL) itr.nextElement();
				// logger.info("findContainingJar finds url:" + url);
				if ("jar".equals(url.getProtocol())) {
					String toReturn = url.getPath();
					if (toReturn.startsWith("file:")) {
						toReturn = toReturn.substring("file:".length());
					}
					toReturn = URLDecoder.decode(toReturn, "UTF-8");
					return toReturn.replaceAll("!.*$", "");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

    public static byte[] getBytes(BytesWritable val) {
        
        byte[] buffer = val.getBytes();
        
        /* FIXME: remove the following part once the below gira is fixed
         * https://issues.apache.org/jira/browse/HADOOP-6298
         */
        long len = val.getLength();
        byte [] bytes = buffer;
        if (len < buffer.length) {
            bytes = new byte[(int) len];
            System.arraycopy(buffer, 0, bytes, 0, (int)len);
        }
        
        return bytes;
    }
    
	private static class MultiPathFilter implements PathFilter {
		private List<PathFilter> filters;

		public MultiPathFilter(List<PathFilter> filters) {
			this.filters = filters;
		}

		public boolean accept(Path path) {
			for (PathFilter filter : filters) {
				if (!filter.accept(path)) {
					return false;
				}
			}
			return true;
		}
	}
	
	public static Path[] getInputPaths(String dirs) {
		String[] list = StringUtils.split(dirs);
		Path[] result = new Path[list.length];
		for (int i = 0; i < list.length; i++) {
			result[i] = new Path(StringUtils.unEscapeString(list[i]));
		}
		return result;
	}
		  
	public static List<FileStatus> listStatus(JobConf job) throws IOException {
		return listStatus(job, FileInputFormat.getInputPaths(job));
	}

	public static List<FileStatus> listStatus(JobConf job, Path[] dirs) throws IOException, InvalidInputException {
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// get tokens for all the required FileSystems..
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);

		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();

		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(PATH_FILTER);
		PathFilter jobFilter = FileInputFormat.getInputPathFilter(job);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);

		for (Path p : dirs) {
			FileSystem fs = p.getFileSystem(job);
			FileStatus[] matches = fs.globStatus(p, inputFilter);
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
			} else {
				for (FileStatus globStat : matches) {
					if (globStat.isDir()) {
						for (FileStatus stat : fs.listStatus(globStat.getPath(), inputFilter)) {
							result.add(stat);
						}
					} else {
						result.add(globStat);
					}
				}
			}
		}

		return result;
	}
	
	public static String humanReadableByteCount(long bytes) {
	    int unit = 1000;
	    if (bytes < unit) return bytes + " B";
	    int exp = (int) (Math.log(bytes) / Math.log(unit));
	    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), ""+ "KMGTPE".charAt(exp-1));
	}
	
	public static int getClientBufferSize(Props props) throws Exception {
		return props.getInt(CLIENT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
	}

	public static int getClientTimeout(Props props) throws Exception {
		return props.getInt(CLIENT_TIMEOUT, DEFAULT_TIMEOUT);
	}


}
