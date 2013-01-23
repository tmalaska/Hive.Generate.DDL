package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This is a very common simple map/reduce job.  It will take any delimitered text file
 * and copy it.  
 * <br><br>
 * It will sort and partition by what every columns you tell it too.
 * <br><br>
 * It also give the called the options of coping to a uncompressed text file or a snappy or gzip sequence file.
 * 
 * 
 * @author ted.malaska
 *
 */
public class CopySortPartJob {

	public static final String DELIMITER_CONF = "copy.job.delimiter";
	public static final String SORT_COLUMN_CONF = "copy.job.sort.column";
	private static final String PARTITION_COLUMN_CONF = "copy.job.partition.column";
	
	public static class CustomMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Text newKey = new Text();
		Text newValue = new Text();

		String delimiter = "";
		Pattern delimiterPattern = null;
		ArrayList<Integer> sortColumns = new ArrayList<Integer>();
		

		@Override
		public void setup(Context context) {
			delimiter = context.getConfiguration().get(DELIMITER_CONF);
			
			if (delimiter.equals("|")) {
				delimiterPattern = Pattern.compile("\\|");
			} else {
				delimiterPattern = Pattern.compile(delimiter);	
			}
			
			String[] sortColumnStrings = context.getConfiguration().get(SORT_COLUMN_CONF).split(",");
			for (String s: sortColumnStrings) {
				sortColumns.add(Integer.parseInt(s));
			}
		}

		long counter = 0;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder builder = new StringBuilder();
			String[] cells = delimiterPattern.split(value.toString());
			
			for (int i: sortColumns) {
				if (cells.length > i) {
					builder.append(cells[i] + delimiter);
				} else {
					builder.append(Math.random() + delimiter);
				}
			}
			newKey.set(builder.toString());
			context.write(newKey, value);
		}
	}

	public static class CustomReducer extends
			Reducer<Text, Text, NullWritable, Text> {

		LongWritable newKey = new LongWritable(0);
		
		@Override
		public void setup(Context context) {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text text: values){
				context.write(NullWritable.get(), text);
			}
		}
	}
	
	public static class CustomPartitioner  extends Partitioner<Text, Text> implements Configurable  {

		String delimiter = "";
		Pattern delimiterPattern = null;
		ArrayList<Integer> partitionColumns = new ArrayList<Integer>();
		
		public void setConf(Configuration conf) {
			delimiter = conf.get(DELIMITER_CONF);
			
			if (delimiter.equals("|")) {
				delimiterPattern = Pattern.compile("\\|");
			} else {
				delimiterPattern = Pattern.compile(delimiter);	
			}
			
			String[] partitionColumnStrings = conf.get(PARTITION_COLUMN_CONF).split(",");
			
			for (String s: partitionColumnStrings) {
				partitionColumns.add(Integer.parseInt(s));	
			}
			
			
		}
		
		@Override
		public int getPartition(Text key, Text value, int numPartitions)
		{
			if (numPartitions == 1)
			{
				return 0;
			} else
			{
				String[] cells = delimiterPattern.split(value.toString());
				
				StringBuilder builder = new StringBuilder();
				
				for (int i: partitionColumns) {
					if (cells.length > i) {
						builder.append(cells[i] + delimiter);
					} else {
						builder.append(cells[0] + delimiter);
					}
						
				}
				int result = (builder.toString()).hashCode() % numPartitions;
				return Math.abs(result);	
			}
			
		}

		public Configuration getConf() {
			// TODO Auto-generated method stub
			return null;
		}


	}

	public static void main(String[] args) throws Exception {
		if (args.length != 7) {
			System.out.println("CopySortPartJob <inputPath> <outputPath> <numOfReducers> <compression> <delimiter> <sortColumns> <partitionColumn>");
			System.out.println();
			System.out.println("Example: CopySortPartJob ./input ./output 2 snappy \\| 0,1 0");
			return;
		}

		// Get values from args
		String inputPath = args[0];
		String outputPath = args[1];
		int numOfReducers = Integer.parseInt(args[2]);
		String compressionCodec = args[3];
		String delimiter = args[4];
		String sortColumns = args[5];	
		String partitionColumn = args[6];

		System.out.println("Input Path:" + inputPath);
		System.out.println("Output Path:" + outputPath);
		System.out.println("Num of Reducers:" + numOfReducers);
		System.out.println("CompressionCodec:" + compressionCodec);
		System.out.println("Delimiter:" + delimiter);
		System.out.println("sortColumn:" + sortColumns);
		System.out.println("partitionColumn:" + partitionColumn);

		// Create job
		Job job = new Job();
		
		job.getConfiguration().set(CopySortPartJob.DELIMITER_CONF, delimiter);
		job.getConfiguration().set(CopySortPartJob.SORT_COLUMN_CONF, sortColumns);
		job.getConfiguration().set(CopySortPartJob.PARTITION_COLUMN_CONF, partitionColumn);

		job.setJarByClass(BackPortCompareJob.class);
		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));

		// Define output format and path
		if (compressionCodec.equals("none")) {
			job.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			
		} else {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
			if (compressionCodec.toLowerCase().equals("gzip")) {
				SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);	
			} else {
				SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			}
				
		}

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setOutputKeyClass(NullWritable.class);		
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(numOfReducers);

		// Exit
		job.waitForCompletion(true);
	}


}
