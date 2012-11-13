package com.cloudera.sa.hive.gen.script.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RDBSchema {
	
	private Map<String, Column> columnMap = new HashMap<String, Column>();
	private List<Column> columnList = new ArrayList<Column>();
	private String tableName;
	private String originalDataFormat;
	
	public List<Column> getColumns() {
		return columnList;
	}
	
	public Column getColumn(String name) {
		return columnMap.get(name);
	}
	
	public List<Column> getPartitionColumns() {
		List<Column> partitionList = new ArrayList<Column>();
		
		for (Column c: columnList) {
			if (c.isPartition) {
				partitionList.add(c);
			}
		}
		
		return partitionList;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	
	
	public String getOriginalDataFormat() {
		return originalDataFormat;
	}

	public void setOriginalDataFormat(String originalDataFormat) {
		this.originalDataFormat = originalDataFormat;
	}

	public void addColumn(String name, String type, int length, boolean isPartition, boolean isPrimaryKey) {
		Column column = new Column(name, type, length, isPartition, isPrimaryKey);
		columnList.add(column);
		columnMap.put(name, column);
	}
	
	public static class Column {
		String name;
		String type;
		int length;
		boolean isPartition;
		boolean isPrimaryKey;
		
		public Column(String name, String type, int length, boolean isPartition, boolean isPrimaryKey) {
			super();
			this.name = name;
			this.type = type;
			this.length = length;
			this.isPartition = isPartition;
			this.isPrimaryKey = isPrimaryKey;
		}
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public int getLength() {
			return length;
		}
		public void setLength(int length) {
			this.length = length;
		}

		public boolean isPartition() {
			return isPartition;
		}

		public void setPartition(boolean isPartition) {
			this.isPartition = isPartition;
		}

		public boolean isPrimaryKey() {
			return isPrimaryKey;
		}

		public void setPrimaryKey(boolean isPrimaryKey) {
			this.isPrimaryKey = isPrimaryKey;
		}
		
		
		
	}
}
