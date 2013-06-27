package com.cloudera.sa.hive.utils;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;


public class RcFileReadSpeedTest {

	public static class CustomMapper
			implements
			Mapper<LongWritable, BytesRefArrayWritable, Text, BytesRefArrayWritable> {
		Text newKey = new Text();
		Text newValue = new Text();

		long byteCounter = 0;

		public void configure(JobConf job) {

		}

		public void close() throws IOException {
			System.out.println("byteCounter:" + byteCounter);

		}

		public void map(LongWritable key, BytesRefArrayWritable value,
				OutputCollector<Text, BytesRefArrayWritable> output,
				Reporter reporter) throws IOException {

			for (int i = 0; i < value.size(); i++) {
				BytesRefWritable cell = value.get(i);
				// We are only looking for a perfect match
				byteCounter += cell.getData().length;
			}
		}
	}

	public static void main(String[] args) throws IOException {

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		ArrayList<Path> inputPaths = new ArrayList<Path>();

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		if (hdfs.isDirectory(inputPath) == false) {
			inputPaths.add(inputPath);
		} else {
			FileStatus[] fileStatuses = hdfs.listStatus(inputPath);
			boolean containsFolders = false;
			for (FileStatus fileStatus : fileStatuses) {
				if (fileStatus.isDirectory()) {
					containsFolders = true;
				}
			}
			if (containsFolders) {
				for (FileStatus fileStatus : fileStatuses) {
					inputPaths.add(fileStatus.getPath());
				}
			} else {
				inputPaths.add(inputPath);
			}
		}

		JobConf conf = new JobConf(new Configuration(),
				PartitionCompactor.class);

		// hadoop
		conf.setJobName("PartitionCompactor");
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(BytesRefArrayWritable.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(BytesRefArrayWritable.class);

		conf.setMapperClass(CustomMapper.class);
		conf.setInputFormat(RCFileInputFormat.class);

		conf.setNumReduceTasks(0);

		conf.set("mapred.output.compress", "false");

		for (Path path : inputPaths) {
			RCFileInputFormat.addInputPath(conf, path);
		}

		conf.setOutputFormat(RCFileOutputFormat.class);
		RCFileOutputFormat.setOutputPath(conf, outputPath);

		RunningJob job = JobClient.runJob(conf);
		job.waitForCompletion();
	}

}
