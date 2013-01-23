package com.cloudera.sa.hive.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This is a simple job that mv files into a given folder.  While moving this job will 
 * prepend a string to the front of the file name.  
 * <br><br>
 * This comes in handle when trying to combine files from two different map/reduce jobs.
 * Given that map/reduce job produce files when names that would conflict if added to the same folder
 * 
 * @author ted.malaska
 *
 */
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
