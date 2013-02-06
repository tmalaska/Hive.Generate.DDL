package com.cloudera.sa.hive.avro.gen;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HiveAvroCreateTableGen 
{
	DataFileWriter d;
	
    public static void main( String[] args ) throws IOException
    {
        if (args.length != 2) {
        	System.out.println("HiveAvroCreateTableGen");
        	System.out.println("Parameters: <hdfs file path> <local hive create script file path>");
        	
        	return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path inputFilePath = new Path(inputFile);

		FSDataInputStream dataInputStream = hdfs.open(inputFilePath);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		//writer.setSchema(s); // I guess I don't need this
		
		DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(dataInputStream, reader);
		try {
			Schema s = dataFileReader.getSchema();
			
			System.out.println("-------------------");
			System.out.println("Avro Schema:");
			System.out.println("-------------------");
			System.out.println(s.getName() + " " + s);
			
			String hiveScript = generateCreateTableScript(s);
			
			System.out.println("-------------------");
			System.out.println("Create table script:");
			System.out.println("-------------------");
			System.out.println(hiveScript);
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			try {
				writer.write(hiveScript);
			} finally {
				writer.close();	
			}
			
		} finally {
			dataFileReader.close();
		}
	    
    }
    
    public static String generateCreateTableScript(Schema s) {
    	
    	String lineSeparator = System.getProperty("line.separator");
    	
    	StringBuilder strBuilder = new StringBuilder();
    	
    	strBuilder.append("CREATE TABLE " + s.getName() + lineSeparator);
    	strBuilder.append("ROW FORMAT " + lineSeparator + 
                          "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " + lineSeparator + 
                          "STORED AS " + lineSeparator + 
                          "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " + lineSeparator + 
                          "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " + lineSeparator + 
                          "TBLPROPERTIES ('avro.schema.literal'=' " + lineSeparator + s.toString() + "');");
    	
    	return strBuilder.toString();
    }
}
