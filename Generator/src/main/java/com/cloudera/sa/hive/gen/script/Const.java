package com.cloudera.sa.hive.gen.script;

public class Const {

	public static final String TEMP_POSTFIX = "_TEMP";
	public static final String BACKPORT_POSTFIX = "_BACKPORT";
	
	public static final String EXISTING_TEMP_POST_DIR_NAME = "_EXISTING_TEMP";
	public static final String DELTA_TEMP_POST_DIR_NAME = "_DELTA_TEMP";
	
	public static final String TEMP_TABLE_ROW_FORMAT = "temp.table.row.format";
	public static final String TEMP_TABLE_STORED_AS = "temp.table.stored.as";
	public static final String TEMP_TABLE_ADD_JARS = "temp.table.add.jars";
	public static final String IS_LOAD_FROM_HDFS = "is.load.from.hdfs";
	
	public static final String DELETE_TEMP_TABLE_DATA_AFTER_LOAD = "delete.temp.table.external.data.after.load";
	public static final String DROP_TEMP_TABLE_AFTER_LOAD = "drop.temp.table.after.load";
	
	public static final String INSERT_INTO_LOGIC = "insert.into.logic";
	public static final String INSERT_INTO_LOGIC_NORMAL = "normal";
	public static final String INSERT_INTO_LOGIC_HIVE8_SIM = "hive8Sim";
	public static final String INSERT_INTO_LOGIC_DELTA = "delta";
	
	public static final String COMPACTOR_NUM_OF_REDUCER = "compactor.num.of.reducer";
	
	public static final String DATETIME_FORMAT = "datetime.format";
	public static final String DATE_FORMAT = "date.format";
	public static final String OTHER_DATE_FORMAT = "other.date.format";
	
	public static final String TRIM_STRING_VALUES = "trim.string.values";
	
	public static final String COMPRESSION_CODEC = "root.compression.codec";
	
	public static final String APPLY_TERADATA_FORMATTING = "apply.teradata.formatting";
	
	public static final String SKIP_COPY_STEP = "skip.copy.step";
	
	public static final String ROOT_EXTERNAL_LOCATION = "root.external.location";
	public static final String ROOT_EXTERNAL_LOCATION_GROUP = "root.external.location.group";
	public static final String ROOT_EXTERNAL_LOCATION_PERMISSIONS = "root.external.location.permissions";
	
	public static final String CHECK_FOR_HEADER_RECORDS = "check.for.header.records";
	
	public static final String USE_DATABASE = "use.database";
}
