package com.cloudera.sa.hive.gen.script;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.cloudera.sa.hive.gen.script.pojo.RDBSchema;
import com.cloudera.sa.hive.gen.script.pojo.RDBSchema.Column;


public class ScriptGenerator {
	
	public static String lineSeparator = System.getProperty("line.separator");
	
	
	public static String generateHiveTable(RDBSchema schema, Properties prop) {
		List<Column> columns = schema.getColumns();
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION);
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("CREATE ");
		
		if (externalLocation != null && externalLocation.isEmpty() == false) {
			builder.append("EXTERNAL ");
		}
		
		builder.append("TABLE " + schema.getTableName() + lineSeparator);
		builder.append("( " );
		
		boolean isFirstColumn = true;
		for (Column c: columns) {
			if (c.isPartition() == false) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(",");
				}
				builder.append(lineSeparator);
				builder.append("   " + c.getName() + "\t" + convertColumnType(c) );
			}
		}
		
		builder.append(") " +  lineSeparator);
		
		List<Column> partitionColumns = schema.getPartitionColumns();
		if (partitionColumns.size() > 0) {
			builder.append("PARTITIONED BY (");
			
			isFirstColumn = true;
			for (Column c: partitionColumns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(",");
				}
				builder.append(c.getName() + " " + convertColumnType(c));
			}
			
			builder.append(")" + lineSeparator);
		}
		
		builder.append(" STORED AS RCFile");
	    
		
		if (externalLocation != null && externalLocation.isEmpty() == false) {
			builder.append(" LOCATION \\\"" + externalLocation + "/" + schema.getTableName() + "\\\"" + lineSeparator);
		}
		
	    
	    builder.append(";");
		
		return builder.toString();
	}
	
	public static String generateTempHiveTable(RDBSchema schema, Properties prop) {
		
		String externalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION, "");
		String addJars = prop.getProperty(Const.TEMP_TABLE_ADD_JARS, "");
		
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		if (addJars.isEmpty() == false) {
			builder.append("ADD JAR " + addJars + ";" + lineSeparator + lineSeparator);
		}
		
		builder.append("CREATE ");
		
		if (externalLocation.isEmpty() == false) {
			builder.append("EXTERNAL ");
		}
		
		builder.append("TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + lineSeparator);
		builder.append("( " );
		
		boolean isFirstColumn = true;
		for (Column c: columns) {
			if (c.isPartition() == false) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(",");
				}
				builder.append(lineSeparator);
				builder.append("   " + c.getName() + "\tSTRING" );
			}
		}
		
		builder.append(")" + lineSeparator);
		
		builder.append(prop.getProperty(Const.TEMP_TABLE_ROW_FORMAT) + lineSeparator);
		
		
		builder.append(prop.getProperty(Const.TEMP_TABLE_STORED_AS));
		
		if (externalLocation.isEmpty() == false) {
			builder.append(" LOCATION \\\"" + externalLocation + "/" + schema.getTableName() + Const.TEMP_POSTFIX + "\\\"" + lineSeparator);
		}
		
	    
	    builder.append(";");
		
		return builder.toString();
	}	
	
	public static String generateDropTempHiveTable(RDBSchema schema, Properties prop) {
		
		String deleteTempFolder = prop.getProperty(Const.DELETE_TEMP_TABLE_DATA_AFTER_LOAD, "false");
		String rootExternalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION, "");
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("hive -e \"DROP TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + ";\"");
		
		
		return builder.toString();
	}
	
	public static String generateDeleteTempTableCommend(RDBSchema schema, Properties prop) {

		String deleteTempFolder = prop.getProperty(Const.DELETE_TEMP_TABLE_DATA_AFTER_LOAD, "false");
		String rootExternalLocation = prop.getProperty(Const.ROOT_EXTERNAL_LOCATION, "");
		String insertIntoMode = prop.getProperty(Const.INSERT_INTO_LOGIC, Const.INSERT_INTO_LOGIC_NORMAL);
		String newLine = System.getProperty("line.separator");
		
		StringBuilder builder = new StringBuilder();
		
		if (deleteTempFolder.equals("true") && rootExternalLocation.isEmpty() == false) {
			
			if (insertIntoMode.equals(Const.INSERT_INTO_LOGIC_HIVE8_SIM)) {
				builder.append("hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.EXISTING_TEMP_POST_DIR_NAME + newLine);
			} else if (insertIntoMode.equals(Const.INSERT_INTO_LOGIC_DELTA)) {
				builder.append("hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.EXISTING_TEMP_POST_DIR_NAME + newLine);
				builder.append("hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.DELTA_TEMP_POST_DIR_NAME + newLine);
			}
			builder.append("hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.TEMP_POSTFIX + newLine);
		}
		return builder.toString();
	}
	
	public static String generateInsertInto(RDBSchema schema, Properties prop) {
		
		String dateFormat = prop.getProperty(Const.DATE_FORMAT, "yyyy.MM.dd");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("SET hive.exec.compress.output=true;" + lineSeparator); 
		builder.append("SET io.seqfile.compression.type=BLOCK;" + lineSeparator);
		builder.append("SET mapred.output.compression.codec = org.apache.hadoop.io.compress.SnappyCodec;" + lineSeparator);
		builder.append(lineSeparator);
		
		builder.append("INSERT INTO TABLE " + schema.getTableName() + " ");
		
		List<Column> partitionColumns = schema.getPartitionColumns();
		
		if (partitionColumns.size() > 0) {
			builder.append("PARTITION (");
			
			boolean isFirstColumn = true;
			for (Column c: partitionColumns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(", ");
				}
				builder.append("a." + c.getName());
			}
			builder.append(")");
		}
		builder.append(lineSeparator + "SELECT " + lineSeparator);
		
		List<Column> columns = schema.getColumns();
		if (columns.size() > 0) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(", " + lineSeparator);
				}
				builder.append("    ");
				
				String type = c.getType().toUpperCase();
				if (type.equals("VARCHAR2") || type.equals("CHAR")) {
					builder.append("a." + c.getName());	
				} else if (type.equals("DATE")) {
					//unix_timestamp(string date, string pattern)
					builder.append("from_unixtime(unix_timestamp(a." + c.getName() + ", '" + dateFormat + "'))");
				} else if (type.equals("NUMBER")) {
					if (c.getLength() > 18) {
						builder.append("a." + c.getName());
					} else if (c.getLength() > 9) {
						builder.append("cast(a." + c.getName() + " as BIGINT)");
					} else if (c.getLength() > 4) {
						builder.append("cast(a." + c.getName() + " as INT)");
					} else if (c.getLength() > 2) {
						builder.append("cast(a." + c.getName() + " as SMALLINT)");
					} else {
						builder.append("cast(a." + c.getName() + " as TINYINT)");
					}
				}
				
			}
			builder.append(lineSeparator);
		}
		
		builder.append("FROM " + schema.getTableName() + Const.TEMP_POSTFIX + " a;");
		
		return builder.toString();	
	}

	
	
	private static String convertColumnType(Column column) {
		String dbType = column.getType().toUpperCase();
		
		if (dbType.equals("VARCHAR2") || dbType.equals("CHAR")) {
			return "STRING";
		} else if (dbType.equals("NUMBER")) {
			if (column.getLength() > 18) {
				return "STRING";
			} else if (column.getLength() > 9) {
				return "BIGINT";
			} else if (column.getLength() > 4) {
				return "INT";
			} else if (column.getLength() > 2) {
				return "SMALLINT";
			} else {
				return "TINYINT";
			}
		} else if (dbType.equals("DATE")) {
			return "TIMESTAMP";
		} else {
			throw new RuntimeException("Currently doesn't support " + dbType);
		}	
	}

	public static String generateLoadOverwrite(RDBSchema schema, Properties prop) {
	
		String isLoadExternal = prop.getProperty(Const.IS_LOAD_FROM_HDFS);
		String newLine = System.getProperty("line.separator");
		
		if (isLoadExternal == null || isLoadExternal.isEmpty() || isLoadExternal.toLowerCase().equals("true") == false) {
			return "FILES=$@ " + newLine + 
					"for f in $FILES " + newLine + 
					"do " + newLine + 
					"  echo \"Loading $f file...\" " + newLine + 
					"  hive -e \"LOAD DATA LOCAL INPATH \\\"$f\\\" INTO TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + ";\"" + newLine + 
					"done " + newLine + newLine;
		} else {
			return "hive -e \"LOAD DATA INPATH \\\"$f\\\" INTO TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + ";\"" + newLine + newLine;
		}
	}
	
	public static String generateTestData(RDBSchema schema, Properties prop, int startingLine, int linesOfData, char strChar) {
		
		String dateFormat = prop.getProperty(Const.DATE_FORMAT, "yyyy.MM.dd");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		for (int i = startingLine; i < linesOfData; i++) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append("|");
				}
				String type = c.getType().toUpperCase();
				if (type.equals("VARCHAR2") || type.equals("CHAR")) {
					
					for (int j = 0; j < c.getLength(); j++) {
						
						builder.append(strChar);
					}
				} else if (type.equals("NUMBER")) {
					StringBuilder numBuilder = new StringBuilder();
					for (int j = 0; j < c.getLength(); j++) {
						if (j == 0) { numBuilder.append("1"); }
						else { numBuilder.append("0"); }
					}
					BigInteger bi = new BigInteger(numBuilder.toString());
					bi = bi.add(new BigInteger("" + i));
					builder.append(bi.toString());
					
				} else if (type.equals("DATE")) {
					
					Date newDate = new Date();
					builder.append(simpleDateFormat.format(newDate));
				}
			}
			if (i < linesOfData - 1) {
				builder.append(lineSeparator);
			}
		}
		return builder.toString();
	}
}
