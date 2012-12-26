package com.cloudera.sa.hive.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

public class UnCompressor {

	public static int finishedThreaded = 0;
	
	public static void main(String[] args) throws IOException, InterruptedException {

		if (args.length < 3) {
			System.out.println("UnCompressor Help:");
			System.out
					.println("Parameters: <inputFilePath> <outputPath> <numOfThreads>");
			System.out.println();
			return;
		}

		String inputLocation = args[0];
		String outputLocation = args[1];
		int numOfThreads = Integer.parseInt(args[2]);
		

		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(numOfThreads, numOfThreads, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(100, true));;
		

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);

		ArrayList<Path> pathArray = new ArrayList<Path>();
		
		if (hdfs.isDirectory(inputFilePath)) {
			RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(inputFilePath, false);
			while(files.hasNext()) {
				LocatedFileStatus lfs = files.next();
				pathArray.add(lfs.getPath());
			}
		} else {
			pathArray.add(inputFilePath);
		}
		
		for (Path path: pathArray) {
			UnCompresserThread thread = new UnCompresserThread(path, outputLocation, hdfs);
			System.out.println("Starting thread for file: " + path);
			threadPool.execute(thread);
		}
		
		while (finishedThreaded < pathArray.size()) {
			
			Thread.sleep(1000);
		}
		System.out.println("Finished all files");
		System.exit(0);
	}
	
	public static class UnCompresserThread implements Runnable {

		Path inputFilePath;
		String outputLocation;
		FileSystem hdfs;
		
		public UnCompresserThread(Path inputFilePath, String outputLocation, FileSystem hdfs) {
			this.inputFilePath = inputFilePath;
			this.outputLocation = outputLocation;
			this.hdfs = hdfs;
		}
		
		public void run() {
			try {
				if (inputFilePath.getName().endsWith(".zip")) {
					uncompressZipFile(outputLocation, hdfs, inputFilePath);	
				} else if (inputFilePath.getName().endsWith("gz") || inputFilePath.getName().endsWith("gzip")) {
					uncompressGzipFile(outputLocation, hdfs, inputFilePath);	
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("Finished thread for file: " + inputFilePath);
			UnCompressor.finishedThreaded++;
		}
		
		private void uncompressGzipFile (String outputLocation,
				FileSystem hdfs, Path inputFilePath) throws IOException {
			
			FSDataInputStream inputStream = hdfs.open(inputFilePath);
			
			GZIPInputStream gzipStream = new GZIPInputStream(inputStream);

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					gzipStream));
			try {

				String newFileName = inputFilePath.getName();
				newFileName = newFileName.substring(0, newFileName.lastIndexOf(".gz"));
				
				Path outputFilePath = new Path(outputLocation + "/" + newFileName);

				FSDataOutputStream outputStream = hdfs.create(outputFilePath);
				
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
				
				try {

					int singleChar = -1;
					singleChar = reader.read();
					while (singleChar > -1) {
						writer.write(singleChar );
						singleChar = reader.read();
					}
				} finally {
					if (writer != null) {
						writer.close();
						writer = null;
					}
				}

			} finally {
				if (reader != null) {
					reader.close();
					reader = null;
				}
			}
		}

		
		private void uncompressZipFile(String outputLocation,
				FileSystem hdfs, Path inputFilePath) throws IOException {
			ZipInputStream zipReader = getZipReader(hdfs, inputFilePath);

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					zipReader));
			try {


				long counter = 0;
				ZipEntry ze;
				while ((ze = zipReader.getNextEntry()) != null) {
					String entryName = ze.getName();
					System.out.println("Entry Name: " + entryName + " "
							+ ze.getSize());

					Path outputFilePath = new Path(outputLocation + "/" + entryName);

					FSDataOutputStream outputStream = hdfs.create(outputFilePath);
					
					BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));
					
					try {

						int singleChar = -1;
						singleChar = reader.read();
						while (singleChar > -1) {
							writer.write(singleChar );
							singleChar = reader.read();
						}
					} finally {
						if (writer != null) {
							writer.close();
							writer = null;
						}
					}
				}

				System.out.println("Finished: Processed " + counter + " lines.");

			} finally {
				if (reader != null) {
					reader.close();
					reader = null;
				}
			}
		}

		public static ZipInputStream getZipReader(FileSystem hdfs, Path path)
				throws IOException {
			FSDataInputStream inputStream = hdfs.open(path);

			if (path.getName().endsWith("zip") ) {
				ZipInputStream zipInputStream = new ZipInputStream(inputStream);

				System.out.println("processing zip file");

				return zipInputStream;
			} else {
				throw new IOException(
						"UnKnown compress type.  Can only process files with ext of (zip)");
			}
		}
		
	}



	public static SequenceFile.Writer getSequenceFileWriter(
			Configuration config, FileSystem hdfs, Path path,
			String compressionCodecStr) throws IOException {
		// Created our writer
		SequenceFile.Metadata metaData = new SequenceFile.Metadata();

		EnumSet<CreateFlag> enumSet = EnumSet.of(CreateFlag.CREATE);
		return SequenceFile.createWriter(FileContext.getFileContext(), config,
				path, NullWritable.class, Text.class,
				SequenceFile.CompressionType.BLOCK,
				getCompressionCodec(compressionCodecStr), metaData, enumSet);
	}

	public static CompressionCodec getCompressionCodec(String value) {

		if (value.equals("snappy")) {
			return new SnappyCodec();
		} else if (value.equals("gzip")) {
			return new GzipCodec();
		} else if (value.equals("bzip2")) {
			return new BZip2Codec();
		} else {
			return new SnappyCodec();
		}
	}

}
