package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Hello world!
 * 
 */
public class BackPortCompareJob {

	
	public static final String DELIMITER_CONFIG = "backport.delimiter";
	public static final String PRIMARY_KEYS_CONFIG = "backport.primaryKeys";
	public static final String GOLD_SRC_CONFIG = "backport.input.gold.src";
	public static final String BACK_PORT_SRC_CONFIG = "backport.input.backport.src";

	public static class CustomMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Text newKey = new Text();
		Text newValue = new Text();

		String delimiter;
		Pattern delimiterSplit;

		int[] keyIndexes;
		String[] keyTypes;

		Pattern collionSplit = Pattern.compile(":");

		Path splitPath;

		boolean isGoldSrc = false;
		String goldFlag = "B";

		SimpleDateFormat oracleDateFormat = new SimpleDateFormat(
				"dd/MM/yyyy HH:mm:ss");
		SimpleDateFormat teradataDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat hiveDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		@Override
		public void setup(Context context) {
			delimiter = context.getConfiguration().get(DELIMITER_CONFIG);
			delimiterSplit = Pattern.compile(delimiter);

			String primaryKeys = context.getConfiguration().get(
					PRIMARY_KEYS_CONFIG);
			String goldSrcPath = context.getConfiguration()
					.get(GOLD_SRC_CONFIG);
			String backPortSrcPath = context.getConfiguration().get(
					BACK_PORT_SRC_CONFIG);

			String[] keysAndTypes = primaryKeys.split(",");
			keyIndexes = new int[keysAndTypes.length];
			keyTypes = new String[keysAndTypes.length];

			for (int i = 0; i < keysAndTypes.length; i++) {
				String keyType = keysAndTypes[i];
				String parts[] = collionSplit.split(keyType);
				keyIndexes[i] = Integer.parseInt(parts[0]);
				keyTypes[i] = parts[1];
			}
			FileSplit fileSplit = (FileSplit) context.getInputSplit();

			splitPath = fileSplit.getPath();
			if (splitPath.toString().contains(goldSrcPath)) {
				isGoldSrc = true;
				goldFlag = "G";
			}

		}

		long counter = 0;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] cells = delimiterSplit.split(value.toString());

			StringBuilder keyBuilder = new StringBuilder();

			for (int i = 0; i < keyIndexes.length; i++) {
				int index = keyIndexes[i];
				String cell = cells[index].trim();

				if (isGoldSrc) {
					try {
						keyBuilder.append(formatGoldKey(keyTypes[i], cell));
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						throw new RuntimeException(e);
					}
				} else {
					keyBuilder.append(cell);
				}
				if (delimiter.equals("\\|")) {
					keyBuilder.append("|");
				} else {
					keyBuilder.append(delimiter);
				}
			}

			newKey.set(keyBuilder.toString());
			newValue.set(goldFlag + value);
			context.write(newKey, newValue);

			counter++;

			if (counter < 5) {
				System.out.println("c0:" + cells[0] + "-" + cells[1] + "-"
						+ cells[2]);
				try {
					System.out.println("cf:"
							+ formatGoldKey("INTEGER", cells[0]));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println("k:" + keyBuilder.toString());
				System.out.println("v:" + goldFlag + value);
			}
		}

		public String formatGoldKey(String type, String value)
				throws ParseException {
			if (type.equals("VARCHAR2") || type.equals("CHAR")
					|| type.equals("VARCHAR")) {
				return value.trim();

			} else if (type.equals("DATE")) {
				if (value.contains("-")) {
					return ""
							+ hiveDateFormat.format(teradataDateFormat
									.parse(value + " 00:00:00"));
				} else {
					return ""
							+ hiveDateFormat.format(oracleDateFormat
									.parse(value + " 00:00:00"));
				}
			} else if (type.equals("DATETIME")) {
				if (value.contains("-")) {
					return ""
							+ hiveDateFormat.format(teradataDateFormat
									.parse(value));
				} else {
					return ""
							+ hiveDateFormat.format(oracleDateFormat
									.parse(value));
				}
			} else if (type.equals("NUMBER")) {
				return value;
			} else if (type.equals("DECIMAL")) {
				if (value.endsWith(".")) {
					return value + "0";
				}
			} else if (type.equals("BYTEINT") || type.equals("SMALLINT")
					|| type.equals("INTEGER") || type.equals("BIGINT")) {
				int index = value.indexOf(".");
				if (index > -1) {
					return value.substring(0, index);
				} else {
					return value;
				}
			}
			throw new RuntimeException("Unknown type: " + type);
		}
	}

	public static class CustomReducer extends
			Reducer<Text, Text, LongWritable, Text> {
		SimpleDateFormat oracleDateFormat = new SimpleDateFormat(
				"dd/MM/yyyy HH:mm:ss");
		SimpleDateFormat teradataDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat hiveDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		LongWritable newKey = new LongWritable(0);
		Text newValue = new Text();

		String delimiter;
		Pattern delimiterSplit;

		@Override
		public void setup(Context context) {
			delimiter = context.getConfiguration().get(DELIMITER_CONFIG);
			delimiterSplit = Pattern.compile(delimiter);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int rowCounter = 0;

			int goldCounter = 0;
			int backPortCounter = 0;

			ArrayList<String[]> goldRecordList = new ArrayList<String[]>();
			ArrayList<String[]> backPortRecordList = new ArrayList<String[]>();

			context.getCounter("compare", "unique key").increment(1);

			for (Text text : values) {
				rowCounter++;
				if (rowCounter < 100) {
					String value = text.toString();
					String[] cellValues = delimiterSplit.split(value
							.substring(1));

					if (value.charAt(0) == 'G') {
						goldCounter++;
						goldRecordList.add(cellValues);
					} else {
						backPortCounter++;
						backPortRecordList.add(cellValues);
					}
					context.getCounter("compare", "unique row").increment(1);
				}
			}

			if (goldCounter != backPortCounter) {
				// We don't have equal number of matches
				newValue.set(key.toString() + ":G=" + goldCounter + ",B="
						+ backPortCounter);
				context.write(newKey, newValue);
			} else {
				// We have matching records by primary key now let us check the
				// values.
				for (String[] backPortRecord : backPortRecordList) {
					for (String[] goldRecord : goldRecordList) {

						for (int i = 0; i < backPortRecord.length; i++) {
							String bpVal = "NA";
							String gVal = "NA";
							try {
								bpVal = backPortRecord[i].trim();
								gVal = goldRecord[i].trim();

								if (bpVal.equals("\\N")) {
									bpVal = "";
								}

								if (bpVal.equals(gVal)) {
									context.getCounter("compare",
											"perfect match").increment(1);
								} else {
									if (bpVal.toLowerCase().equals("null")) {
										bpVal = "";
									}
									if (bpVal.equals(gVal)) {
										context.getCounter("compare",
												"null match").increment(1);
									} else {
										try {
											if (bpVal.contains(".")) {
												// Then we have double
												if (gVal.endsWith(".")) {
													gVal = gVal + "0";
												}
												bpVal = ""
														+ Double.parseDouble(bpVal);
												gVal = ""
														+ Double.parseDouble(gVal);
											} else {
												int index = gVal.indexOf(".");
												if (index > -1) {
													gVal = gVal.substring(0,
															index);
												}
											}
										} catch (Exception e) {
											// numberformat exception
										}
									}
									if (bpVal.equals(gVal)) {
										context.getCounter("compare",
												"number match").increment(1);
									} else {
										if (gVal.contains(":")) {
											if (gVal.contains("-")) {
												gVal = hiveDateFormat
														.format(teradataDateFormat
																.parse(gVal));
											} else if (gVal.contains("/")) {
												gVal = hiveDateFormat
														.format(oracleDateFormat
																.parse(gVal));
											}
										} else {
											if (gVal.contains("-")) {
												gVal = hiveDateFormat
														.format(teradataDateFormat
																.parse(gVal
																		+ " 00:00:00"));
											} else if (gVal.contains("/")) {
												gVal = hiveDateFormat
														.format(oracleDateFormat
																.parse(gVal
																		+ " 00:00:00"));
											}
										}

									}
									if (bpVal.equals(gVal)) {
										context.getCounter("compare",
												"date match").increment(1);
									} else {
										context.getCounter("compare",
												"Fail Match").increment(1);
										newValue.set(key.toString() + ":C=" + i
												+ "G=" + gVal + ",B=" + bpVal);
										context.write(newKey, newValue);
									}
								}
							} catch (Exception e) {
								context.getCounter("compare", e.getClass() + "")
										.increment(1);
								newValue.set("Ex: " + key.toString() + ":C="
										+ i + "G=" + gVal + ",B=" + bpVal);
								context.write(newKey, newValue);
							}
						}

					}
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 6) {
			System.out
					.println("BackPortCompareJob <goldSrcInput> <backPortSrcInput> <outputPath> <# reducers> <delimier> <primaryKeys>");
			System.out.println();
			System.out
					.println("Example: BackPortCompareJob ./inputGold ./inputBp ./output 2 \"|\" 1:INT,2:STRING");
			return;
		}

		// Get values from args
		String inputGoldPath = args[0];
		String inputBpPath = args[1];
		String outputPath = args[2];
		String numberOfReducers = args[3];
		String delimiter = args[4];
		String primaryKeys = args[5];

		System.out.println("Input Gold Path:" + inputGoldPath);
		System.out.println("Input BackPort Path:" + inputBpPath);
		System.out.println("Output Path:" + outputPath);
		System.out.println("Delimiter:" + delimiter);
		System.out.println("Primary Keys:" + primaryKeys);

		// Create job
		Job job = new Job();

		job.getConfiguration().set(BackPortCompareJob.DELIMITER_CONFIG,
				delimiter);
		job.getConfiguration().set(BackPortCompareJob.PRIMARY_KEYS_CONFIG,
				primaryKeys);
		job.getConfiguration().set(BackPortCompareJob.BACK_PORT_SRC_CONFIG,
				inputBpPath);
		job.getConfiguration().set(BackPortCompareJob.GOLD_SRC_CONFIG,
				inputGoldPath);

		job.setJarByClass(BackPortCompareJob.class);
		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(inputGoldPath));
		TextInputFormat.addInputPath(job, new Path(inputBpPath));

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(numberOfReducers));

		// Exit
		job.waitForCompletion(true);
	}
}
