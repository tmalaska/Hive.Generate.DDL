import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class IoTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
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
	}

}
