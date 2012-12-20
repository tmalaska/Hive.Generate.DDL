package com.cloudera.sa.hive.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MoveAddPrePend {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		String fromFolder = args[0];
		String toFolder = args[1];
		String prefix = args[2];


		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		
		FileStatus[] rootFolderFiles = hdfs.listStatus(new Path(fromFolder));
		
		
		for (FileStatus fs: rootFolderFiles) {
			String filename = fs.getPath().getName();
			if (filename.startsWith("_") == false) {
				Path newFilePath = new Path(toFolder + "/" + prefix + filename);
				
				System.out.println("Moving " + fs.getPath() + " to " + newFilePath);
				
				hdfs.rename(fs.getPath(), newFilePath);
			}
		}
		
		
		
		System.out.println("Done");
		
	}

}
