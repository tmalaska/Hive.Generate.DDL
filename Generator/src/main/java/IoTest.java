import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class IoTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		/*

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		FileStatus fs = hdfs.getFileStatus(new Path(args[0]));
		System.out.println("Name:" + fs.getPath().getName());
		System.out.println("Path:" + fs.getPath());
		System.out.println("Uri:" + fs.getPath().toUri());
		System.out.println("UriPath:" + fs.getPath().toUri().getPath());
		System.out.println("UriRawPath:" + fs.getPath().toUri().getRawPath());
		hdfs.close();
*/
		System.out.println( Double.parseDouble("1234567891.1234567891"));
		System.out.println( Double.parseDouble("993456789.923456789"));
		System.out.println( Double.parseDouble("1234567.1234567"));
		System.out.println( Double.parseDouble("12345678.12345678"));
		String a = "12|34";
		//System.out.println(a.replaceAll("\\.[0-9]*", "z"));
		System.out.println(a.split("\\|").length);
		SimpleDateFormat teradataDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(teradataDateFormat.parse("2010-01-01" + " 00:00:00"));
	}

}
