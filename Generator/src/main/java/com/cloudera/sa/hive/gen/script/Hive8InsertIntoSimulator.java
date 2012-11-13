package com.cloudera.sa.hive.gen.script;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Hive8InsertIntoSimulator {

	public static final String ROOT_DIR_TEMP_POST_NAME = Const.EXISTING_TEMP_POST_DIR_NAME;
	public static final String COPY_STRING = "_copy_";
	
	/**
	 * <Stage> <tableName> <possibleExternalLocation>
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
		}else {
			FileStatus rootFolderStatus = hdfs.getFileStatus(rootFolder);
			
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
		
		hdfs.close();
		System.out.println("----------------------------- Finished " + stage);
	}

}
