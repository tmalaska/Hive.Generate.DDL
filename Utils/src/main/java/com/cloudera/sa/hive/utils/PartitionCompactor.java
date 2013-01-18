package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.Mapper;

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

		RCFileInputFormat.addInputPath(conf, new Path(existingInputPath));
		RCFileInputFormat.addInputPath(conf, new Path(deltaInputPath));

		conf.setOutputFormat(RCFileOutputFormat.class);
		RCFileOutputFormat.setOutputPath(conf, new Path(outputPath));

		RunningJob job = JobClient.runJob(conf);
		job.waitForCompletion();

	}

}
