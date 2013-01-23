package com.cloudera.sa.hive.utils;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Hive8InsertIntoSimulator {

	public static final String ROOT_DIR_TEMP_POST_NAME = "_EXISTING_TEMP";
	
	public static final String COPY_STRING = "_copy_";
	
	
	/**
	 * In Hive 8 the insert into commend does do what it should.  Instead it does a insert override.  
	 * This class will simulate what happens in Hive 9 insert into.  That is it will add the string
	 * "_copy_{num of copy}" to the end of the existing files after the new files are added.
	 * <br><br>
	 * This code will handle both partitioned and non-partitioned tables.
	 * <br><br>
	 * This code was not intended to be executed manually.  The hive.gen.script jar will generate load scripts
	 * that will call this class at the right steps in the process.
	 * <br><br>
	 * The args for this main class are as follows:
	 * {stage} {tableName} {possibleExternalLocation}
	 * <Stage> <tableName> <possibleExternalLocation>
	 * <br><br>
	 * 1. Stage: can have one of two values: prep, reinsert.  Prep will move the existing data out pf the tables folder.  
	 * reinsert will do just that it will move the files back into the table folders, making sure to append the _copy_# string 
	 * to the end of the files as to not conflict with any new files added to the table's folder.
	 * 
	 * 2. tableName: This is the name of the folder in which you wish to append too.
	 * 
	 * 3. possibleExternalLocation: In case you are using a external table.  This will allow you to store the temp files in
	 * a external location of your choosing.  This is important for security reasons.  Because you want the temp folder to be
	 * in the same parent folder as the original table.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		String stage = args[0];
		String tableName = args[1];
		String rootPath = "/user/hive/warehouse";
		if (args.length > 1) {
			rootPath = args[2];
		}
		
		// Set up hdfs
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path rootFolder = new Path(rootPath + "/" + tableName);
		Path tempRootFolder = new Path(rootPath + "/" + tableName + ROOT_DIR_TEMP_POST_NAME);
		
		System.out.println("----------------------------- Starting " + stage);
		if (stage.equals("prep")) {
			System.out.println();
			System.out.println("Moving " + rootFolder + " to " + tempRootFolder);
			hdfs.rename(rootFolder, tempRootFolder);
			System.out.println("Making " + rootFolder);
			hdfs.mkdirs(rootFolder);
			System.out.println(hdfs.listStatus(rootFolder).length + " files is " + rootFolder);
			System.out.println(hdfs.listStatus(tempRootFolder).length + " files is " + tempRootFolder);
		}else if (stage.equals("reinsert")){
			
			if (tableIsPartitioned(hdfs, tempRootFolder)) {
				// We are dealing with a partitioned table
				System.out.println("Reinserting partitions");	
				reinsertAllPartitions(hdfs, rootFolder, tempRootFolder);
			} else {
				System.out.println("Reinserting non-partition table data");
				// We are dealing with a unpartitioned table
				reinsertPartition(hdfs, rootFolder, tempRootFolder);	
			}
			
			
		} else {
			System.out.println("Unknow Stage: Doing Nothing");
		}
		
		hdfs.close();
		System.out.println("----------------------------- Finished " + stage);
	}
	
	public static boolean tableIsPartitioned(FileSystem hdfs, Path tempRootFolder) throws FileNotFoundException, IOException {
		FileStatus[] tempRootFolderFiles = hdfs.listStatus(tempRootFolder);
		if (tempRootFolderFiles.length > 0) {
			return tempRootFolderFiles[0].isDirectory();
		}
		return false;
	}
	
	private static void reinsertAllPartitions(FileSystem hdfs, Path rootFolder, Path tempRootFolder) throws FileNotFoundException, IOException {
		FileStatus[] tempRootFolderFiles = hdfs.listStatus(tempRootFolder);
		for (FileStatus fs: tempRootFolderFiles) {
			if (fs.isDirectory() == false) {
				throw new RuntimeException("Found file " + fs.getPath() + " in root directory while tring to reinsert partitions");
			}
			String dirName = fs.getPath().getName();
			Path rootPartitionDirectory = new Path(rootFolder + "/" + dirName);
			
			if (hdfs.exists(rootPartitionDirectory) == false) {
				//The partition doesn't exist so we have to recreate the directory
				hdfs.mkdirs(rootPartitionDirectory);
			}
			
			reinsertPartition(hdfs, rootPartitionDirectory, fs.getPath());
		}
	}

	private static void reinsertPartition(FileSystem hdfs, Path rootFolder,
			Path tempRootFolder) throws FileNotFoundException, IOException {
		FileStatus[] rootFolderFiles = hdfs.listStatus(rootFolder);
		FileStatus[] tempRootFolderFiles = hdfs.listStatus(tempRootFolder);
		
		int highestCopy = 0;
		
		//Get highest copy number
		for (FileStatus fs: tempRootFolderFiles) {
			String filename = fs.getPath().getName();
			int idx = filename.indexOf(COPY_STRING);
			if (idx > -1) {
				int copyNum = Integer.parseInt(filename.substring(idx + COPY_STRING.length()));
				if (copyNum >= highestCopy) {
					highestCopy = copyNum + 1;
				}
			}
		}
		
		System.out.println("Stage A - highest copy " + highestCopy);
		
		//Rename newly added files with copy_<highestCopy>
		for (FileStatus fs: rootFolderFiles) {
			String filePath = fs.getPath().toUri().getPath();
			Path newFilePath = new Path(filePath + COPY_STRING + highestCopy);
			
			System.out.println("Stage B - Moving " + fs.getPath() + " to " + newFilePath);
			
			hdfs.rename(fs.getPath(), newFilePath);
		}
		
		//Moves initial files back into root table
		for (FileStatus fs: tempRootFolderFiles) {
			String filename = fs.getPath().getName();
			
			Path tempFile = new Path(tempRootFolder + "/" + filename);
			Path rootFile = new Path(rootFolder + "/" + filename);
			
			System.out.println("Stage C - Moving " + tempFile + " to " + rootFile);
			
			hdfs.rename(tempFile, rootFile);
		}
		
		hdfs.delete(tempRootFolder, true);
	}

}
