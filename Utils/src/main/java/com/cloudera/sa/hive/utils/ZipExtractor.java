package com.cloudera.sa.hive.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.EnumSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;

public class ZipExtractor {

	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("ZipExtractor Help:");
			System.out
					.println("Parameters: <inputFilePath> <outputPath>");
			System.out.println();
			return;
		}

		String inputLocation = args[0];
		String outputLocation = args[1];

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path inputFilePath = new Path(inputLocation);

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
