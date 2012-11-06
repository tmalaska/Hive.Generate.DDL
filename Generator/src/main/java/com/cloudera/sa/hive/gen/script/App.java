package com.cloudera.sa.hive.gen.script;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVReader;

import com.cloudera.sa.hive.gen.script.pojo.RDBSchema;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {	
    	if (args.length != 3) {
    		System.out.println("hiveGen - help");
    		System.out.println("");
    		System.out.println("example:");
    		System.out.println("hadoop jar hiveGen.jar <csv file path> <outputDir> <proerties file location>");
    		return;
    	}
        //CSVReader reader = new CSVReader(new FileReader("/Users/ted.malaska/Documents/workspace/hive.gen.script/samples/bigSample.csv"));
    	CSVReader reader = new CSVReader(new FileReader(args[0]));
        //String outputDirector = "/Users/ted.malaska/Documents/workspace/hive.gen.script/output/";
    	String outputDirector = args[1];
    	
    	Properties prop = new Properties();
    	prop.load(new FileInputStream(new File(args[2])));
    	
        String [] nextLine;
        
        RDBSchema schema = new RDBSchema();
        schema.setOriginalDataFormat("|");
        
        String currentTable = "";
        
        boolean isHeaderRow = true;
        while ((nextLine = reader.readNext()) != null) {
            if (isHeaderRow) {
            	isHeaderRow = false;
            } else {
            	
            	String tableName = nextLine[0];
            	
            	if(currentTable.equals(tableName) == false) { 
            		if (currentTable.isEmpty() == false) {
            			tableFileOutput(schema, outputDirector, prop);
            			tableConsoleOutput(schema, prop);
            		}
            		currentTable = tableName;
            		schema = new RDBSchema();
                    schema.setOriginalDataFormat("|");
            	}
            	
            	schema.setTableName(tableName);
            	
            	int length = 0;
            	if (nextLine[4].isEmpty() == false) {
            		length = Integer.parseInt(nextLine[4]);
            	}
            	
            	schema.addColumn(nextLine[2], nextLine[3], length, false);
            	
            }
        	
        	
        }
        tableFileOutput(schema, outputDirector, prop);
        tableConsoleOutput(schema, prop);
        
        reader.close();
        
    }

    private static void tableFileOutput(RDBSchema schema, String directory, Properties prop) throws IOException {
    	FileWriter writerC = new FileWriter(new File(directory + "/" + schema.getTableName() + "_Create.sh"));
    	ScriptGenerator.lineSeparator = " ";
    	writerC.write("hive -e \"" + ScriptGenerator.generateHiveTable(schema, prop) + "\"");
    	writerC.close();

    	
    	FileWriter writerL = new FileWriter(new File(directory + "/" + schema.getTableName() + "_Load.sh"));
    	String ls = System.getProperty("line.separator");
    	writerL.write("hive -e \"" + ScriptGenerator.generateTempHiveTable(schema, prop) + "\"");
    	writerL.write(ls + ls);
    	writerL.write(ScriptGenerator.generateLoadOverwrite(schema));
    	writerL.write(ls + ls);
    	writerL.write("hive -e \"" + ScriptGenerator.generateInsertInto(schema) + "\"");
    	writerL.write(ls + ls);
    	writerL.write("hive -e \"" + ScriptGenerator.generateDropTempHiveTable(schema) + "\"");
    	writerL.close();
    	
    	FileWriter writerD = new FileWriter(new File(directory + "/" + schema.getTableName() + "_SampleData.txt"));
    	ScriptGenerator.lineSeparator = System.getProperty("line.separator");
    	writerD.write(ScriptGenerator.generateTestData(schema, 5));
    	writerD.close();
    }
    
	private static void tableConsoleOutput(RDBSchema schema, Properties prop) {

    	ScriptGenerator.lineSeparator = System.getProperty("line.separator");
		System.out.println("----------------------");
		System.out.println("-- Step1: Create Hive Temp Table");
        System.out.println(ScriptGenerator.generateTempHiveTable(schema, prop));
        System.out.println();
        System.out.println("-- Step2: Create Hive Table");
        System.out.println(ScriptGenerator.generateHiveTable(schema, prop));
        System.out.println();
        System.out.println("-- Step3: Load Data to Temp Table");
        System.out.println(ScriptGenerator.generateLoadOverwrite(schema));
        System.out.println();
        System.out.println("-- Step4: Insert Data into Hive Table");
        System.out.println(ScriptGenerator.generateInsertInto(schema));
        System.out.println();
        System.out.println("-- Step5: Drop Temp Table");
        System.out.println(ScriptGenerator.generateDropTempHiveTable(schema));
        System.out.println();
        System.out.println("-----------------------");
	}
}
