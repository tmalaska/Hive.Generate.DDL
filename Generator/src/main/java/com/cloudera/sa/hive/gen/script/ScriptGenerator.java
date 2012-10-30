package com.cloudera.sa.hive.gen.script;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.cloudera.sa.hive.gen.script.pojo.RDBSchema;
import com.cloudera.sa.hive.gen.script.pojo.RDBSchema.Column;


public class ScriptGenerator {
	
	public static String lineSeparator = System.getProperty("line.separator");
	public static String dateFormat = "yyyy.MM.dd";
	public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
	
	public static String generateHiveTable(RDBSchema schema) {
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("CREATE TABLE " + schema.getTableName() + lineSeparator);
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
		
		builder.append(")");
		
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
		
	    builder.append(" STORED AS RCFile;");
		
		return builder.toString();
	}
	
	public static String generateTempHiveTable(RDBSchema schema) {
		
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("CREATE TABLE " + schema.getTableName() + "_TEMP" + lineSeparator);
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
		
		String originalFormat = schema.getOriginalDataFormat().toUpperCase();
		if (originalFormat.equals("CSV")) {
			builder.append("ROW FORMAT SERDE 'com.bizo.hive.serde.csv.CSVSerde'" + lineSeparator);	
		} else { 
			builder.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + originalFormat + "'" + lineSeparator);
		}
		
		builder.append(" STORED AS TextFile;");
		
		return builder.toString();
	}	
	
	public static String generateDropTempHiveTable(RDBSchema schema) {
		return "DROP TABLE " + schema.getTableName() + "_TEMP;";
	}
	
	public static String generateInsertInto(RDBSchema schema) {
		
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
		
		builder.append("FROM " + schema.getTableName() + "_TEMP a;");
		
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

	public static String generateLoadOverwrite(RDBSchema schema) {
		
		return "LOAD DATA LOCAL INPATH \\\"$1\\\" OVERWRITE INTO TABLE " + schema.getTableName() + "_TEMP";
	}
	
	public static String generateTestData(RDBSchema schema, int linesOfData) {
		List<Column> columns = schema.getColumns();
		
		StringBuilder builder = new StringBuilder();
		
		for (int i = 0; i < linesOfData; i++) {
			boolean isFirstColumn = true;
			for (Column c: columns) {
				if (isFirstColumn) {
					isFirstColumn = false;
				} else {
					builder.append("|");
				}
				String type = c.getType().toUpperCase();
				if (type.equals("VARCHAR2") || type.equals("CHAR")) {
					char startChar = 'A';
					for (int j = 0; j < c.getLength(); j++) {
						char newChar = (char) ((j%25) + startChar);
						builder.append(newChar);
					}
				} else if (type.equals("NUMBER")) {
					char startChar = '1';
					for (int j = 0; j < c.getLength(); j++) {
						char newChar = (char) ((j%8) + startChar);
						builder.append(newChar);
					}
				} else if (type.equals("DATE")) {
					long timeStamp = (long) (Math.random() * Long.MAX_VALUE);
					Date newDate = new Date(timeStamp);
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
