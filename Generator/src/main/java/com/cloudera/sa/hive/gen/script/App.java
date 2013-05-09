package com.cloudera.sa.hive.gen.script;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.cloudera.sa.hive.gen.script.pojo.RDBSchema;
import com.cloudera.sa.hive.gen.script.pojo.RDBSchema.Column;

/**
 * Hello world!
 *
 */
public class App 
{
	private static Logger logger = Logger.getLogger(App.class);
	
    public static void main( String[] args ) throws IOException
    {	    	  	
    	if (args.length != 3) {
    		System.out.println("hiveGen - help");
    		System.out.println("");
    		System.out.println("example:");
    		System.out.println("java -jar hive.gen.script.jar <csv file path> <outputDir> <proerties file location>");
    		return;
    	}
        //CSVReader reader = new CSVReader(new FileReader("/Users/ted.malaska/Documents/workspace/hive.gen.script/samples/bigSample.csv"));
    	CSVReader reader = new CSVReader(new FileReader(args[0]));

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
            	if (nextLine.length < 3) {
            		logger.warn("encounter invalid line in file, skipping: " + nextLine);
            		continue;
            	}
            	
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
            		length = Integer.parseInt(nextLine[4].trim());
            	}
            	
            	int percistion = 0;
            	if (nextLine[5].isEmpty() == false) {
            		percistion = Integer.parseInt(nextLine[5]);
            	}
            	
            	boolean isPrimaryKey = false;
            	if (nextLine.length > 7 && nextLine[7].equals("Y")) {
            		isPrimaryKey = true;
            	}
            	
            	boolean isPartition = false;
            	if (nextLine.length > 8 && nextLine[8].equals("Y")) {
            		isPartition = true;
            	}
            	
            	schema.addColumn(nextLine[2], nextLine[3], length, percistion, isPartition, isPrimaryKey);
            }
        }
        tableFileOutput(schema, outputDirector, prop);
        tableConsoleOutput(schema, prop);
        
        reader.close();
        
    }

    private static void tableFileOutput(RDBSchema schema, String directory, Properties prop) throws IOException {

    	ScriptGenerator.lineSeparator = " ";
    	
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION);
		String database = prop.getProperty(Const.USE_DATABASE, "default");
		String externalGroup = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_GROUP, "supergroup");
		String externalPermission = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_PERMISSIONS, "770");
    	
    	FileWriter writerC = new FileWriter(new File(directory + "/" + schema.getTableName() + "_Create.sh"));
    	writerC.write(generateMainHiveTableCreatationScript(schema, externalLocation, database, externalGroup, externalPermission));
    	writerC.close();

    	FileWriter writerL = new FileWriter(new File(directory + "/" + schema.getTableName() + "_Load.sh"));
    	writerL.write(generateLoadDataScript(schema, prop));
    	writerL.close();
    	
    	FileWriter writerBp = new FileWriter(new File(directory + "/" + schema.getTableName() + "_BackPort.sh"));
    	writerBp.write(generateBackPortScript(schema, prop));
    	writerBp.close();
    	
    	FileWriter writerChDb = new FileWriter(new File(directory + "/" + schema.getTableName() + "_ChangeDatabase.sh"));
    	writerChDb.write(generateChangeDatabaseScript(schema, prop));
    	writerChDb.close();
    	
    	FileWriter writerChED = new FileWriter(new File(directory + "/" + schema.getTableName() + "_ChangeExternalDirectory.sh"));
    	writerChED.write(generateChangeExternalDirectoryScript(schema, prop, false));
    	writerChED.close();
    	
    	FileWriter writerTmpChED = new FileWriter(new File(directory + "/" + schema.getTableName() + "_Temp_ChangeExternalDirectory.sh"));
    	writerTmpChED.write(generateChangeExternalDirectoryScript(schema, prop, true));
    	writerTmpChED.close();
    	
    	ScriptGenerator.lineSeparator = System.getProperty("line.separator");
    	FileWriter writerD = new FileWriter(new File(directory + "/" + schema.getTableName() + "_SampleData.Init.txt"));
    	writerD.write(ScriptGenerator.generateTestData(schema, prop, 0, 12,'A'));
    	writerD.close();
    	
    	if (prop.getProperty(Const.INSERT_INTO_LOGIC, "normal").equals(Const.INSERT_INTO_LOGIC_DELTA) || 
    			prop.getProperty(Const.INSERT_INTO_LOGIC, "normal").equals(Const.INSERT_INTO_LOGIC_HIVE8_SIM)) {
    		FileWriter writerD2 = new FileWriter(new File(directory + "/" + schema.getTableName() + "_SampleData.Delta.txt"));
        	writerD2.write(ScriptGenerator.generateTestData(schema, prop, 6, 18,'B'));
        	writerD2.close();
    	}
    	
    	
    }
    
	private static void tableConsoleOutput(RDBSchema schema, Properties prop) {
		
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION);
		String database = prop.getProperty(Const.USE_DATABASE, "default");
		String externalGroup = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_GROUP, "");
		String externalPermission = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_PERMISSIONS, "");
		
		ScriptGenerator.lineSeparator = System.getProperty("line.separator");
		System.out.println("# Create Script");
		
		
		System.out.println(generateMainHiveTableCreatationScript(schema, externalLocation, database, externalGroup, externalPermission));
		
		System.out.println("# Load Script");
		System.out.println(generateLoadDataScript(schema, prop));
    }

	private static String generateMainHiveTableCreatationScript(
			RDBSchema schema, String externalLocation, String database, String externalGroup, String externalPermission) {
		String ls = System.getProperty("line.separator");

    	
		return 
				"hive -e \"create database " + database + ";\"" + 
				ls + 
				"hive -e \"" + ScriptGenerator.generateHiveTable(schema, externalLocation, database) + "\"" + 
				ls +
				ls +
				ScriptGenerator.generateChExternalDir(schema, externalLocation, externalGroup, externalPermission);
	}
    
	private static String generateChangeDatabaseScript(RDBSchema schema, Properties prop) {
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION);
		String databaseName = prop.getProperty(Const.USE_DATABASE, "default");
		String externalGroup = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_GROUP, "supergroup");
		String externalPermission = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_PERMISSIONS, "770");
		
		StringBuilder builder = new StringBuilder();
		String ls = System.getProperty("line.separator");
		
		builder.append(ls +"echo \"Stage: Drop Existing Table \"" + ls);
		builder.append("hive -e \"drop table " + databaseName + "." + schema.getTableName() + ";\"");
		
		databaseName = "$1";
		builder.append(ls + ls +"echo \"Stage: Create new table in correct database\"" + ls + ls);
		builder.append(generateMainHiveTableCreatationScript(schema, externalLocation, databaseName, externalGroup, externalPermission));
		
		
		return builder.toString();
	}
	
	private static String generateChangeExternalDirectoryScript(RDBSchema schema, Properties prop, boolean isTempTable) {
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION);
		String databaseName = prop.getProperty(Const.USE_DATABASE, "default");
		String externalGroup = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_GROUP, "supergroup");
		String externalPermission = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION_PERMISSIONS, "770");
		String tableName = isTempTable?schema.getTableName() + Const.TEMP_POSTFIX:schema.getTableName();
		
		
		StringBuilder builder = new StringBuilder();
		String ls = System.getProperty("line.separator");
		
		builder.append(ls +"echo \"Stage: Create New External Directory\"" + ls );
		builder.append("hadoop fs -mkdir $1" + ls);
		builder.append("hadoop fs -mv " + externalLocation + "/" + tableName + " $1/" + tableName + ls);
		builder.append("hive -e \"drop table " + databaseName + "." + tableName + ";\"" + ls);
		if (isTempTable) {
			String tempTableStoredAs = prop.getProperty(Const.TEMP_TABLE_STORED_AS);
			String rowFormat = prop.getProperty(Const.TEMP_TABLE_ROW_FORMAT);
			String addJars = prop.getProperty(Const.TEMP_TABLE_ADD_JARS, "");
	    	builder.append("hive -e \"" + ScriptGenerator.generateTempHiveTable(schema, "$1", addJars, rowFormat, tempTableStoredAs) + "\"");
		} else {
			builder.append(generateMainHiveTableCreatationScript(schema, "$1", databaseName, externalGroup, externalPermission));	
		}
		
		
		return builder.toString();
	}
	
	private static String generateBackPortScript(RDBSchema schema, Properties prop) {
		
		String rowFormat = prop.getProperty(Const.TEMP_TABLE_ROW_FORMAT);
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION, "");
		String addJars = prop.getProperty(Const.TEMP_TABLE_ADD_JARS, "");
		
		StringBuilder builder = new StringBuilder();
		String ls = System.getProperty("line.separator");
    	
    	builder.append(ls + "echo \"Stage: Create Temp Table\"" + ls);
    	builder.append("hive -e \"" + ScriptGenerator.generateBackPortHiveTable(schema, externalLocation, addJars, rowFormat) + "\"");
    	builder.append(ls + "echo \"Stage: Insert into Hive Table\"" + ls);
    	builder.append("hive -e \"" + ScriptGenerator.generateBackPort(schema, prop) + "\"");
    	
    	
    	return builder.toString();
	}
	
	private static String generateLoadDataScript(RDBSchema schema, Properties prop) {
    	String insertInfoLogic = prop.getProperty(Const.INSERT_INTO_LOGIC, "normal");
    	String deleteTempTableData = prop.getProperty(Const.DELETE_TEMP_TABLE_DATA_AFTER_LOAD, "false");
    	boolean skipCopyStep = prop.getProperty(Const.SKIP_COPY_STEP, "false").equals("true");
    	boolean dropTempTableAfterLoad = prop.getProperty(Const.DROP_TEMP_TABLE_AFTER_LOAD, "true").equals("true");
		String tempTableStoredAs = prop.getProperty(Const.TEMP_TABLE_STORED_AS);
		String rowFormat = prop.getProperty(Const.TEMP_TABLE_ROW_FORMAT);
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION, "");
		String addJars = prop.getProperty(Const.TEMP_TABLE_ADD_JARS, "");
    	
    	
		StringBuilder builder = new StringBuilder();
		String ls = System.getProperty("line.separator");
    	
    	builder.append(ls + ls +"echo \"Stage: Create Temp Table\"" + ls + ls);
    	builder.append("hive -e \"" + ScriptGenerator.generateTempHiveTable(schema, externalLocation, addJars, rowFormat, tempTableStoredAs) + "\"");
    	
    	if (skipCopyStep == false) {
	    	builder.append(ls + ls +"echo \"Stage: Loading data into Temp Table\""  + ls);
	    	builder.append(ScriptGenerator.generateLoadOverwrite(schema, prop));
	    	builder.append(ls + "echo \"Stage: Preping\"" + ls);
    	}
    	
    	if (insertInfoLogic.equals(Const.INSERT_INTO_LOGIC_HIVE8_SIM)) {
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.Hive8InsertIntoSimulator prep " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    		
    	} else if (insertInfoLogic.equals(Const.INSERT_INTO_LOGIC_DELTA)) {
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.PartitionStager ePrep " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    		
    	}  
    	
    	builder.append(ls + "echo \"Stage: Insert into Hive Table\"" + ls);
    	builder.append("hive -e \"" + ScriptGenerator.generateInsertInto(schema, prop) + "\"");
    	
    	builder.append(ls + ls +"echo \"Stage: Additional Prep and Staging\"" + ls);
    	
    	if (insertInfoLogic.equals(Const.INSERT_INTO_LOGIC_HIVE8_SIM)) {
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.Hive8InsertIntoSimulator reinsert " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    		builder.append(ls + ls);
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.Hive8InsertIntoSimulator cleanup " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    	} else if (insertInfoLogic.equals(Const.INSERT_INTO_LOGIC_DELTA)) {
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.PartitionStager dPrep " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    		builder.append(ls + ls);
    		
    		//<existing Input Path> <delta Input Path> <primaryKeyList> <outputPath> <# reducers>
    		String tablePath = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  "/user/hive/warehouse") + "/" + schema.getTableName();
    		String existingInputPath = tablePath + Const.EXISTING_TEMP_POST_DIR_NAME;
    		String deltaInputPath = tablePath + Const.DELTA_TEMP_POST_DIR_NAME;
    		String primaryKeyList = generateCommonSepartatedPrimaryKeyList(schema);
    		String maxColumns = "" + schema.getColumns().size();
    		String outputPath = tablePath;
    		String numOfReducers = prop.getProperty(Const.COMPACTOR_NUM_OF_REDUCER, "1");
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.PartitionCompactor " + 
    				existingInputPath + " " +
    				deltaInputPath + " " +
    				primaryKeyList + " " +
    				maxColumns + " " + 
    				outputPath + " " + 
    				numOfReducers);
    		
    		builder.append(ls + ls);
    		builder.append("hadoop jar hive.loader.utils.jar com.cloudera.sa.hive.utils.PartitionStager cleanup " + schema.getTableName() + " " + prop.getProperty(Const.ROOT_EXTERNAL_LOCATION,  ""));
    		builder.append(ls + ls);
    	}  
    	
    	if (dropTempTableAfterLoad) {
	    	builder.append(ls + ls +"echo \"Stage: Drop Hive Temp Table\"" + ls);
	    	builder.append( ScriptGenerator.generateDropTempHiveTable(schema, prop) );
    	}
    	
    	if (deleteTempTableData.equals("true")) {
        	builder.append(ls + ls +"echo \"Stage: Delete Temp Table Data From HDFS\"" + ls);
        	builder.append( ScriptGenerator.generateDeleteTempTableCommend(schema, prop) );	
    	}
    	return builder.toString();
	}
	
	private static String generateCommonSepartatedPrimaryKeyList(RDBSchema schema) {
		StringBuilder builder = new StringBuilder();
		int counter = 0;
		int primaryKeys = 0;
		for (Column column: schema.getColumns()) {
			if (column.isPrimaryKey()) {
				if (primaryKeys > 0) {
					builder.append(",");
				}
				builder.append(counter);
				
				primaryKeys++;
			}
			counter++;
		}
		
		if (primaryKeys == 0) {
			builder.append("0");
		}
		return builder.toString();
	}

}
