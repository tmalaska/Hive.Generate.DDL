package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This is a very handle map/reduce job for validating delimiter files.  
 * <br><br>
 * It will read through the file and report how many rows have how many delimiters.
 * <br><br>
 * It will also write out a sampling of rows that do not match the desired delimiter count.
 * 
 * @author ted.malaska
 *
 */
public class DelimiterIssueFinderJob {


	public static final String DELIMITER_CONFIG = "backport.delimiter";
	public static final String EXPECTED_NUMBER_OF_COLUMNS_CONFIG = "backport.expected..number.of.columns";

	public static class CustomMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Text newKey = new Text();
		Text newValue = new Text();
		
		String delimiter;
		int expectedNumOfColumns;
		Pattern delimiterPattern = null;
		

		@Override
		public void setup(Context context) {
			delimiter = context.getConfiguration().get(DELIMITER_CONFIG);
			expectedNumOfColumns = context.getConfiguration().getInt(EXPECTED_NUMBER_OF_COLUMNS_CONFIG, 0);
			
			if (expectedNumOfColumns < 1) {
				throw new RuntimeException("expectedNumOfColumns is 0");
			}
			
			if (delimiter.equals("|")) {
				delimiter = "\\|";
			}
			
			delimiterPattern = Pattern.compile(delimiter);
		}

		HashSet<Integer> columnCountSet = new HashSet<Integer>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int columnCount = delimiterPattern.split(value.toString()).length;
			
			context.getCounter("ColumnCheck", "Columns " + columnCount).increment(1);
			
			if (columnCount != expectedNumOfColumns) {
				
				if (columnCountSet.add(columnCount)) {
					newKey.set("ColCount:" + columnCount);
					context.write(newKey, value);
				}
			}
			
		}

		
	}

	

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.out
					.println("DelimiterIssueFinderJob <inputPath> <outputPath> <delimiter> <expectedNumOfColumns>");
			System.out.println();
			System.out
					.println("Example: DelimiterIssueFinderJob ./input ./output ~ 5");
			return;
		}

		// Get values from args
		String inputPath = args[0];
		String outputPath = args[1];
		String delimiter = args[2];
		String expectedNumOfColumns = args[3];

		System.out.println("Input Path:" + inputPath);
		System.out.println("Output Path:" + outputPath);
		System.out.println("Delimter:" + delimiter);
		System.out.println("Expected Num Of Columns:" + expectedNumOfColumns);

		// Create job
		Job job = new Job();

		job.getConfiguration().set(DelimiterIssueFinderJob.DELIMITER_CONFIG,
				delimiter);
		job.getConfiguration().set(DelimiterIssueFinderJob.EXPECTED_NUMBER_OF_COLUMNS_CONFIG,
				expectedNumOfColumns);

		job.setJarByClass(BackPortCompareJob.class);
		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputPath));

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		//job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		// Exit
		job.waitForCompletion(true);
	}


}
