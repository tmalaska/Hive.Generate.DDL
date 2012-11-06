package com.cloudera.sa.hive.bigbigint;

import java.math.BigInteger;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ConvertToBinaryToStrNum extends UDF {
	public Text evaluate(final Text val, final IntWritable numOfBytes) {
		BigInteger bi = new BigInteger(val.copyBytes());
		
		Text result = new Text();
		result.set(bi.toString());
		return result;
	}
}
