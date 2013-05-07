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
	
	
	public static String generateChExternalDir(RDBSchema schema, String externalLoc, String externalGroup, String externalPermission) {
		
		String ls = System.getProperty("line.separator");
		
		if (externalLoc == null) {
			externalLoc = "";
		}
		
		if (  externalLoc.isEmpty() == false) {
			
			
			
			StringBuilder builder = new StringBuilder();
			if (externalGroup.isEmpty() == false) {
				builder.append("hadoop fs -chgrp " + externalGroup + " " + externalLoc + "/" + schema.getTableName()   + ls  + ls);
			}
			if (externalPermission.isEmpty() == false) {
				builder.append("hadoop fs -chmod " + externalPermission + " " + externalLoc + "/" + schema.getTableName() +   ls  + ls);
			}
			return builder.toString();
		} else {
			return "";
		}
	}
	
	public static String generateHiveTable(RDBSchema schema, String externalLocation, String database) {
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("CREATE ");
		
		if (externalLocation != null && externalLocation.isEmpty() == false) {
			builder.append("EXTERNAL ");
		}
		
		builder.append("TABLE " + database + "." + schema.getTableName() + lineSeparator);
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
	
	public static String generateTempHiveTable(RDBSchema schema, String externalLocation, String addJors, String rowFormat, String tempTableStoredAs) {
		return generateBasicStringHiveTable(schema, externalLocation, addJors, Const.TEMP_POSTFIX, rowFormat, tempTableStoredAs);
	}
	
	public static String generateBackPortHiveTable(RDBSchema schema, String externalLocation, String addJars, String rowFormat) {
		return generateBasicStringHiveTable(schema, externalLocation, addJars, Const.BACKPORT_POSTFIX, rowFormat, "STORED AS TEXTFILE");
	}
	
	public static String generateBasicStringHiveTable(RDBSchema schema, String externalLocation, String addJars, String postFix, String rowFormat, String storedAs) {

		
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		if (addJars.isEmpty() == false) {
			builder.append("ADD JAR " + addJars + ";" + lineSeparator + lineSeparator);
		}
		
		builder.append("CREATE ");
		
		if (externalLocation.isEmpty() == false) {
			builder.append("EXTERNAL ");
		}
		
		builder.append("TABLE " + schema.getTableName() + postFix + lineSeparator);
		builder.append("( " );
		
		boolean isFirstColumn = true;
		for (Column c: columns) {
			//if (c.isPartition() == false) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(",");
				}
				builder.append(lineSeparator);
				builder.append("   " + c.getName() + "\tSTRING" );
			//}
		}
		
		builder.append(")" + lineSeparator);
		
		builder.append(rowFormat + lineSeparator);
		
		builder.append(storedAs);
		
		if (externalLocation.isEmpty() == false) {
			builder.append(" LOCATION \\\"" + externalLocation + "/" + schema.getTableName() + postFix + "\\\"" + lineSeparator);
		}
		
	    
	    builder.append(";");
		
		return builder.toString();
	}	
	
	public static String generateDropTempHiveTable(RDBSchema schema, Properties prop) {
		
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
				builder.append(newLine + "hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.EXISTING_TEMP_POST_DIR_NAME + newLine);
			} else if (insertIntoMode.equals(Const.INSERT_INTO_LOGIC_DELTA)) {
				builder.append(newLine + "hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.EXISTING_TEMP_POST_DIR_NAME + newLine);
				builder.append(newLine + "hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.DELTA_TEMP_POST_DIR_NAME + newLine);
			}
			builder.append(newLine + "hadoop fs -rm -r -skipTrash " + rootExternalLocation + "/" + schema.getTableName() + Const.TEMP_POSTFIX + newLine);
		}
		return builder.toString();
	}
	
	public static String  generateBackPort(RDBSchema schema, Properties prop) {
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("INSERT INTO TABLE " + schema.getTableName() + Const.BACKPORT_POSTFIX + " ");
		
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
					
				builder.append("a." + c.getName());
						
			}
			builder.append(lineSeparator);
		}
		
		builder.append("FROM " + schema.getTableName() + " a ");
		
		builder.append(";");
		
		return builder.toString();
	}
	
	public static String generateInsertInto(RDBSchema schema, Properties prop) {
		
		String dateFormat = prop.getProperty(Const.DATE_FORMAT, "dd/MM/yyyy");
		String otherDateFormat = prop.getProperty(Const.OTHER_DATE_FORMAT, "dd/MM/yy");
		String datetimeFormat = prop.getProperty(Const.DATETIME_FORMAT, "dd/MM/yyyy HH:mm:ss");
		boolean applyTeradataFormatting = prop.getProperty(Const.APPLY_TERADATA_FORMATTING, "false").equals("true");
		
		boolean doTrimOnString = prop.getProperty(Const.TRIM_STRING_VALUES, "false").equals("true");
		String compressionCodec = prop.getProperty(Const.COMPRESSION_CODEC, "org.apache.hadoop.io.compress.SnappyCodec");
		
		String blockSize = prop.getProperty(Const.FS_LOCAL_BLOCK_SIZE, "67108864");
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("SET hive.exec.dynamic.partition=true;" + lineSeparator);
        builder.append("SET hive.exec.dynamic.partition.mode=nonstrict;" + lineSeparator); 
		builder.append("SET hive.exec.compress.output=true;" + lineSeparator); 
		builder.append("SET io.seqfile.compression.type=BLOCK;" + lineSeparator);
		builder.append("SET mapred.output.compression.codec = " + compressionCodec + ";" + lineSeparator);
		builder.append("SET fs.local.block.size = " + blockSize + ";" + lineSeparator);
		builder.append("SET hive.io.rcfile.record.buffer.size = " + Math.max((4 * 1024 * 1024), ((schema.getColumns().size() / 3) * 1024 * 1024)) + ";"  + lineSeparator);
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
				builder.append( c.getName());
			}
			builder.append(")");
		}
		builder.append(lineSeparator + "SELECT " + lineSeparator);
		
		List<Column> columns = schema.getColumns();
		if (columns.size() > 0) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (c.isPartition() == false) {
					if (isFirstColumn) {
						isFirstColumn = false;
					} else {
						builder.append(", " + lineSeparator);
					}
					builder.append("    ");
					
					appendColumnForInsertIntoFromTemp(dateFormat,
							otherDateFormat, datetimeFormat,
							applyTeradataFormatting, doTrimOnString, builder, c);
				}
			}
			for (Column c: partitionColumns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append(", " + lineSeparator);
				}
				builder.append("    ");
				
				appendColumnForInsertIntoFromTemp(dateFormat,
						otherDateFormat, datetimeFormat,
						applyTeradataFormatting, doTrimOnString, builder, c);
			}
			
			builder.append(lineSeparator);
		}
		
		builder.append("FROM " + schema.getTableName() + Const.TEMP_POSTFIX + " a ");
		
		if (prop.getProperty(Const.CHECK_FOR_HEADER_RECORDS, "false").equals("true")) {
			builder.append(lineSeparator);
		 	builder.append("WHERE a." + columns.get(0).getName() + " != '" + columns.get(0).getName() + "'");
		}
		
		builder.append(";");
		
		return builder.toString();	
	}

	private static void appendColumnForInsertIntoFromTemp(String dateFormat,
			String otherDateFormat, String datetimeFormat,
			boolean applyTeradataFormatting, boolean doTrimOnString,
			StringBuilder builder, Column c) {
		String type = c.getType().toUpperCase();
		if (type.equals("VARCHAR2") || type.equals("CHAR") || type.equals("VARCHAR") || type.equals("CLOB")) {
			if (doTrimOnString || applyTeradataFormatting) { builder.append("trim("); }
			
			builder.append("a." + c.getName());
			
			if (doTrimOnString || applyTeradataFormatting) { builder.append(")"); }
		} else if (type.equals("DATE")) {
			String fieldName = "a." + c.getName();
			if (applyTeradataFormatting) {
				fieldName = "trim(" + fieldName + ")";
			}
			builder.append("from_unixtime(unix_timestamp(" + fieldName + ", '" + dateFormat + "'))");
		}else if (type.equals("OTHERDATE")) {
			String fieldName = "a." + c.getName();
			if (applyTeradataFormatting) {
				fieldName = "trim(" + fieldName + ")";
			}
			builder.append("from_unixtime(unix_timestamp(" + fieldName + ", '" + otherDateFormat + "'))");
		} else if (type.equals("DATETIME") || type.equals("TIMESTAMP")) {
			String fieldName = "a." + c.getName();
			if (applyTeradataFormatting) {
				fieldName = "trim(" + fieldName + ")";
			}
			
			builder.append("from_unixtime(unix_timestamp(" + fieldName + ", '" + datetimeFormat + "'))");
		} else if (type.equals("NUMBER") || type.equals("DECIMAL") || type.equals("BYTEINT") || type.equals("SMALLINT") || type.equals("INTEGER")|| type.equals("BIGINT") || type.equals("FLOAT")) {
			String fieldName = "a." + c.getName();
			if (c.getPercision() == 0) {
				
				if (applyTeradataFormatting) {
					fieldName = "trim(" + fieldName + ")";
					if (type.equals("DECIMAL")) {
						fieldName = "regexp_replace(" + fieldName + ",\\\"\\\\\\.[0-9]*\\\",\\\"\\\")";
					}
				} 
				if (c.getLength() > 18) {
					builder.append(fieldName);
				} else if (c.getLength() > 9 || type.equals("BIGINT")) {
					builder.append("cast(" + fieldName + " as BIGINT)");
				} else if (c.getLength() > 4 || type.equals("INTEGER")) {
					builder.append("cast(" + fieldName + " as INT)");
				} else if (c.getLength() > 2 || type.equals("SMALLINT")) {
					builder.append("cast(" + fieldName + " as SMALLINT)");
				} else {
					builder.append("cast(" + fieldName + " as TINYINT)");
				}	
				
				
			} else {
				
				if (applyTeradataFormatting) {
					fieldName = "trim(" + fieldName + ")";
					if (type.equals("DECIMAL")) {
						fieldName = "regexp_replace(" + fieldName + ",\\\"^\\\\\\.\\\",\\\"0.\\\")";
					}
				}
				if (c.getLength() > 17) {
					builder.append(fieldName);
				} else {
					builder.append("cast(" + fieldName + " as DOUBLE)");
				}
			}
		}
	}

	
	
	private static String convertColumnType(Column column) {
		String dbType = column.getType().toUpperCase();
		
		
		if (dbType.equals("VARCHAR2") || dbType.equals("CHAR")|| dbType.equals("VARCHAR") || dbType.equals("CLOB")) {
			return "STRING";
		} else if (dbType.equals("NUMBER") || dbType.equals("DECIMAL") || dbType.equals("BYTEINT")  || dbType.equals("SMALLINT") || dbType.equals("INTEGER")|| dbType.equals("BIGINT") || dbType.equals("FLOAT")) {
			if (column.getPercision() == 0) {
				if (column.getLength() > 18) {
					return "STRING";
				} else if (column.getLength() > 9 || dbType.equals("BIGINT")) {
					return "BIGINT";
				} else if (column.getLength() > 4 || dbType.equals("INTEGER")) {
					return "INT";
				} else if (column.getLength() > 2 || dbType.equals("SMALLINT")) {
					return "SMALLINT";
				} else {
					return "TINYINT";
				}	
			} else {
				if (column.getLength() > 17) {
					return "STRING";
				} else {
					return "DOUBLE";
				}
			}
			
		} else if (dbType.equals("OTHERDATE") || dbType.equals("DATE") || dbType.equals("DATETIME") || dbType.equals("TIMESTAMP")) {
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
					"  hive -e \"LOAD DATA LOCAL INPATH \\\"$f\\\" OVERWRITE INTO TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + ";\"" + newLine + 
					"done " + newLine + newLine;
		} else {
			return "hive -e \"LOAD DATA INPATH \\\"$1\\\" OVERWRITE INTO TABLE " + schema.getTableName() + Const.TEMP_POSTFIX + ";\"" + newLine + newLine;
		}
	}
	
	public static String generateTestData(RDBSchema schema, Properties prop, int startingLine, int linesOfData, char strChar) {
		
		String dateFormat = prop.getProperty(Const.DATE_FORMAT, "dd/MM/yyyy");
		String otherDateFormat = prop.getProperty(Const.OTHER_DATE_FORMAT, "dd/MM/yy");
		String datetimeFormat = prop.getProperty(Const.DATETIME_FORMAT, "dd/MM/yyyy HH:mm:ss");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		SimpleDateFormat simpleOtherDateFormat = new SimpleDateFormat(otherDateFormat);
		SimpleDateFormat simpleDateTimeFormat = new SimpleDateFormat(datetimeFormat);
		
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		if (prop.getProperty(Const.CHECK_FOR_HEADER_RECORDS, "false").equals("true")) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append("|");
				}
				builder.append(c.getName());
			}
			builder.append(lineSeparator);
		}
		
		
		for (int i = startingLine; i < linesOfData; i++) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append("|");
				}
				
				if (c.isPartition()) {
					//If the field is a partition field the following code with generate a partition for every 5 rows.
					String type = c.getType().toUpperCase();
					if (type.equals("DATE")) {
						
						Date newDate = new Date();
						newDate.setDate(newDate.getDate() + i/5);
						builder.append(simpleDateFormat.format(newDate));
					} else if (type.equals("OTHERDATE")) {
						
						Date newDate = new Date();
						newDate.setDate(newDate.getDate() + i/5);
						builder.append(simpleOtherDateFormat.format(newDate));
					} else if (type.equals("DATETIME") || type.equals("DATETIME")) {
						
						Date newDate = new Date();
						newDate.setDate(newDate.getDate() + i/5);
						builder.append(simpleDateTimeFormat.format(newDate));
					} else {
						builder.append("" + i/5);
					}
						
				} else {
				
					String type = c.getType().toUpperCase();
					if (type.equals("VARCHAR2") || type.equals("CHAR") || type.equals("CLOB")|| type.equals("VARCHAR") || type.equals("CLOB")) {
						
						for (int j = 0; j < c.getLength(); j++) {
							
							builder.append(strChar);
						}
					} else if (type.equals("NUMBER") || type.equals("DECIMAL") || type.equals("BYTEINT") || type.equals("SMALLINT") || type.equals("INTEGER")|| type.equals("BIGINT") || type.equals("FLOAT")) {
						StringBuilder numBuilder = new StringBuilder();
						for (int j = 0; j < c.getLength(); j++) {
							//add possible percision
							if (c.getPercision() > 0) {
								if (c.getPercision() == c.getLength() - j) {
									numBuilder.append(".");
								}
							}
							//add number
							if (j == 0) { numBuilder.append("1"); }
							else { numBuilder.append("0"); }
						}
						if (c.getPercision() == 0) {
							BigInteger bi = new BigInteger(numBuilder.toString());
							bi = bi.add(new BigInteger("" + i));
								
							builder.append(bi);	
						} else {
							builder.append(numBuilder);
						}
						
						
					} else if (type.equals("DATE")) {
						
						Date newDate = new Date();
						builder.append(simpleDateFormat.format(newDate));
					} else if (type.equals("OTHERDATE")) {
						
						Date newDate = new Date();
						builder.append(simpleOtherDateFormat.format(newDate));
					} else if (type.equals("DATETIME") || type.equals("DATETIME")) {
						
						Date newDate = new Date();
						builder.append(simpleDateTimeFormat.format(newDate));
					}
				}
			}
			if (i < linesOfData - 1) {
				builder.append(lineSeparator);
			}
		}
		return builder.toString();
	}
}
