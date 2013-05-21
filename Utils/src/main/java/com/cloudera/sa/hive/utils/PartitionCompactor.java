package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;


/**
 * This map/reduce job is designed to merge an existing hive tables data with new data.  
 * Joining on the primary key or keys.  If more then one row joins to a primary key or keys.
 * Then the new row will be keeped and the older row will be removed.  The results
 * of this will be a delta table.  Allowing delta to exist in a Hive table even through 
 * the unlining file system doesn't support deltas.
 * <br><br>
 * This map/reduce job will handle partitioned and non-partitioned tables. and has
 * been tested on Hive 8 and 9
 * <br><br>
 * In the case of partitioned tables.  Only the partitions that existing in the existing 
 * data and the new data will go through the map/reduce process.  If the partition only
 * exist in the new of existing data.  Then the data is simply copied to the final table
 * with a move.  This will allow very large tables to be merge quickly if they are partitioned
 * correctly.
 * 
 * @author ted.malaska
 *
 */
public class PartitionCompactor {

	public static final String EXISTING_FILE_PATH_CONF = "custom.existing.file.path";
	public static final String DELTA_FILE_PATH_CONF = "custom.delta.file.path";
	public static final String PRIMARY_KEYS_CONF = "custom.primary.keys";

	public static final char zeroChar = 0;
	
	//With these values the T will come before the f
	public static final char IS_DELTA_TRUE = 'T';
	public static final char IS_DELTA_FALSE = 'f';

	public static class CustomMapper
			implements
			Mapper<LongWritable, BytesRefArrayWritable, Text, BytesRefArrayWritable> {
		Text newKey = new Text();
		Text newValue = new Text();

		char isDeltaChar = IS_DELTA_FALSE;
		int[] primaryKeyIndexes;

		public void configure(JobConf job) {
			String inputFile = job.get("map.input.file");
			String deltaInputPath = job.get(DELTA_FILE_PATH_CONF);

			if (inputFile.contains(deltaInputPath)) {
				isDeltaChar = IS_DELTA_TRUE;
			}

			StringTokenizer st = new StringTokenizer(
					job.get(PRIMARY_KEYS_CONF), ",");
			primaryKeyIndexes = new int[st.countTokens()];

			for (int i = 0; st.hasMoreTokens(); i++) {
				primaryKeyIndexes[i] = Integer.parseInt(st.nextToken());
			}

		}

		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		public void map(LongWritable key, BytesRefArrayWritable value,
				OutputCollector<Text, BytesRefArrayWritable> output,
				Reporter reporter) throws IOException {

			

			StringBuilder keyBuilder = new StringBuilder();
			for (int i = 0; i < primaryKeyIndexes.length; i++) {
				BytesRefWritable cell = value.get(primaryKeyIndexes[i]);
				String cellStr = new String(cell.getData()).substring(cell.getStart(), cell.getStart() + cell.getLength());
				// We are only looking for a perfect match
				keyBuilder.append(cellStr + zeroChar);
			}
			
			keyBuilder.append(isDeltaChar);
			newKey.set(keyBuilder.toString());
			
			output.collect(newKey, value);
		}
	}

	public static class CustomReducer implements
			Reducer<Text, BytesRefArrayWritable, LongWritable, BytesRefArrayWritable> {
		LongWritable newKey = new LongWritable(0);

		String lagPrefixKey = "";
		
		public void configure(JobConf job) {
			// TODO Auto-generated method stub

		}

		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		long counter = 0;
		public void reduce(Text key, Iterator<BytesRefArrayWritable> values,
				OutputCollector<LongWritable, BytesRefArrayWritable> output,
				Reporter reporter) throws IOException {
			
			String keyString = key.toString();
			String keyPrefix = keyString.substring(0, keyString.length() -1);
			
			newKey.set(counter++);
			
			if (keyPrefix.equals(lagPrefixKey) == false) {
				lagPrefixKey = keyPrefix;
				if(values.hasNext()) {
					BytesRefArrayWritable value = values.next();
					BytesRefWritable cell = value.get(0);
					String cellStr = new String(cell.getData()).substring(cell.getStart(), cell.getStart() + cell.getLength());
					
					
					output.collect(newKey, value);
				}
			}
		}
	}

	public static class CustomPartitioner implements
			Partitioner<Text, BytesRefArrayWritable> {

		public void configure(JobConf job) {
			// TODO Auto-generated method stub

		}

		public int getPartition(Text key, BytesRefArrayWritable value,
				int numPartitions) {
			String keyString = key.toString();
			String partKey = keyString.substring(0, keyString.length() -1);
			int iPartition = Math.abs(partKey.hashCode() % numPartitions);
			return iPartition;
		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			System.out
					.println("PartitionCompactor <existing Input Path> <delta Input Path> <primaryKeyList> <maxColumns> <outputPath> <# reducers>");
			System.out.println();
			System.out
					.println("Example: PartitionCompactor existing delta 0,1  8 output 2");
			return;
		}

		// Get values from args
		String existingInputPath = args[0];
		String deltaInputPath = args[1];
		String primaryKeyList = args[2];
		String maxColumns = args[3];
		String outputPath = args[4];
		String numberOfReducers = args[5];

		
		compact(existingInputPath, deltaInputPath,
				primaryKeyList, maxColumns, outputPath, numberOfReducers);
	}
	
	private static void compact(String existingInputPath, 
			String deltaInputPath, String primaryKeyList, String maxColumns,
			String outputPath, String numberOfReducers) throws IOException {
		if (isPartitioned(existingInputPath, deltaInputPath)) {
			compactAllPartitions(existingInputPath, deltaInputPath,
					primaryKeyList, maxColumns, outputPath, numberOfReducers);
		} else {
			compactASinglePartition(existingInputPath, deltaInputPath,
					primaryKeyList, maxColumns, outputPath, numberOfReducers);	
		}
	}

	private static boolean isPartitioned(String existingInputPath, String deltaInputPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		FileStatus[] folders = hdfs.listStatus(new Path(deltaInputPath));
		
		if (folders.length > 0 && folders[0].isDirectory()) {
			return true;
		}

		folders = hdfs.listStatus(new Path(existingInputPath));
		if (folders.length > 0 && folders[0].isDirectory()) {
			return true;
		}
		
		return false;
	}
	
	private static void compactAllPartitions(String existingInputPath, 
			String deltaInputPath, String primaryKeyList, String maxColumns,
			String outputPath, String numberOfReducers) throws IOException {
		
		HashSet<String> deltaFolders = new HashSet<String>();
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		FileStatus[] deltaFolderStatusArray = hdfs.listStatus(new Path(deltaInputPath));
		
		Path outputPathPath = new Path(outputPath);
		
		if (hdfs.exists(outputPathPath) == false) {
			if (hdfs.mkdirs(outputPathPath) == false) {
				throw new RuntimeException("Unable to create " + outputPathPath);
			}
		}
		
		for (FileStatus fs: deltaFolderStatusArray) {
			deltaFolders.add(fs.getPath().getName());
		}
		
		FileStatus[] existingFolderStatusArray = hdfs.listStatus(new Path(existingInputPath));
		for (FileStatus fs: existingFolderStatusArray) {
			if (deltaFolders.contains(fs.getPath().getName())){
				//We have a partition that exist is both the existing data and the delta data
				//So we have to run a compaction mr job.
				System.out.println("Running Compaction for partition:" + fs.getPath().getName());
				
				compact(existingInputPath + "/" + fs.getPath().getName(), 
						deltaInputPath + "/"  + fs.getPath().getName(),
						primaryKeyList, maxColumns, outputPath + "/"  + fs.getPath().getName(), numberOfReducers);
				//Remove the folder from the delta hashSet so we 
				deltaFolders.remove(fs.getPath().getName());
			} else {
				//We have a partition that exist is the existing data but not in the delta data.
				System.out.println("Moving Existing Partition Back into table:" + fs.getPath().getName());
				System.out.println("Moving " + existingInputPath + "/" + fs.getPath().getName() + " to " + outputPath + "/"  + fs.getPath().getName());
				if (hdfs.rename(new Path(existingInputPath + "/" + fs.getPath().getName()), 
						new Path(outputPath + "/"  + fs.getPath().getName())) == false) {
					throw new RuntimeException("Unable to move file");
				}
			}
		}
		
		for (String unCompactedDeltaPartition: deltaFolders) {
			//We have a partition delta partition but not in the existing partition.
			System.out.println("Moving Delta Partition into table without compaction:" + unCompactedDeltaPartition);
			System.out.println("Moving " + deltaInputPath + "/" + unCompactedDeltaPartition + " to " + outputPath + "/"  + unCompactedDeltaPartition);
			
			if (hdfs.rename(new Path(deltaInputPath + "/" + unCompactedDeltaPartition), 
					new Path(outputPath + "/"  + unCompactedDeltaPartition)) == false){
				throw new RuntimeException("Unable to move file");
			}
		
		}
		//Code to add
		
	}
	
	private static void compactASinglePartition(String existingInputPath,
			String deltaInputPath, String primaryKeyList, String maxColumns,
			String outputPath, String numberOfReducers) throws IOException {
		JobConf conf = new JobConf(new Configuration(), PartitionCompactor.class);

		// hadoop
		conf.setJobName("PartitionCompactor");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(BytesRefArrayWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(BytesRefArrayWritable.class);

		conf.setMapperClass(CustomMapper.class);
		conf.setReducerClass(CustomReducer.class);
		conf.setPartitionerClass(CustomPartitioner.class);
		conf.setInputFormat(RCFileInputFormat.class);

		conf.setNumReduceTasks(Integer.parseInt(numberOfReducers));

		conf.set(EXISTING_FILE_PATH_CONF, existingInputPath);
		conf.set(DELTA_FILE_PATH_CONF, deltaInputPath);
		conf.set(PRIMARY_KEYS_CONF, primaryKeyList);
		conf.set(RCFile.COLUMN_NUMBER_CONF_STR, maxColumns);
		
		conf.set("mapred.output.compress", "true");
		conf.set("mapred.output.compression.codec", GzipCodec.class.toString());

		RCFileInputFormat.addInputPath(conf, new Path(existingInputPath));
		RCFileInputFormat.addInputPath(conf, new Path(deltaInputPath));

		conf.setOutputFormat(RCFileOutputFormat.class);
		RCFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		RunningJob job = JobClient.runJob(conf);
		job.waitForCompletion();
	}

}
