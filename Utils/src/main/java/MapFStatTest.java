import java.io.FileInputStream;
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

public class MapFStatTest {




	NativeIO a;
	
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
			
		}

		long counter = 0;

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			FileInputStream fis = new FileInputStream("/etc/hadoop/conf/log4j.properties");
			NativeIO.fstat(fis.getFD()).getGroup();	
			fis.close();
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
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		}
	}

	public static void main(String[] args) throws Exception {

		// Get values from args

		// Create job
		Job job = new Job();

		job.setJarByClass(MapFStatTest.class);
		// Define input format and path
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));

		// Define output format and path
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Define the mapper and reducer
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);

		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(Integer.parseInt(args[2]));

		// Exit
		job.waitForCompletion(true);
	}
}
