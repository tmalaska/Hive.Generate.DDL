package com.cloudera.sa.hive.bigbigint;

import java.math.BigInteger;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class ConvertToStrNumToBinary extends UDF {
	IntWritable defaultNumOfBytes = new IntWritable(16);
	Text result = new Text();
	
	public Text evaluate(final Text val) {
		
		return this.evaluate(val, defaultNumOfBytes);
	}
	
	public Text evaluate(final Text val, final IntWritable numOfBytes) {
		BigInteger bi = new BigInteger(val.toString());
		byte[] bytes = bi.toByteArray();
		
		if (bytes.length > numOfBytes.get()) {
			throw new RuntimeException("Number '" + val + "' can not fit in " + numOfBytes + " bytes.");
		}
		
		byte[] bigBytes = new byte[numOfBytes.get()];
    	
		System.arraycopy(bytes,
                0,
                bigBytes,
                bigBytes.length - bytes.length,
                bytes.length);
		
		result.set(bigBytes);
		return result;
		
	}
}
