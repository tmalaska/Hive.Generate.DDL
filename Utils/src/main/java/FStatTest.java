import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIO.Stat;


public class FStatTest {

	static int successes = 0;
	static int failures = 0;
	static int openFiles = 0;
	static int maxOpenFiles = 0;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName = args[0];
		int numOfThreads = Integer.parseInt(args[1]);
		int runs = Integer.parseInt(args[2]);
		int waitTime = Integer.parseInt(args[3]);
		String fStatFlag = args[4];

		System.out.println("Real test - no sync");
		
		Thread[] tArray = new Thread[numOfThreads];
		
		for (int i = 0; i < numOfThreads; i++) {
			tArray[i] = new Thread(new FStatThread(fileName, runs, waitTime, i, fStatFlag.toLowerCase().equals("t")));
		}
		
		for (int i = 0; i < numOfThreads; i++) {
			tArray[i].start();
		}
		System.out.println("Everything Started");
	}
	
	private static class FStatThread implements Runnable {

		String fileName;
		int runs;
		int waitTime;
		int threadNum;
		boolean fstatFlag;
		int nothing = 0;
		static Object  sync = new Object();
		
		FStatThread(String fileName, int runs, int waitTime, int threadNum, boolean fstatFlag) {
			this.fileName = fileName;
			this.runs = runs;
			this.waitTime = waitTime;
			this.threadNum = threadNum;
			this.fstatFlag = fstatFlag; 
			
		}
		
		public void run() {
			int i = 0;
			FileInputStream fis = null;
			
			for (i = 0; i < runs; i++) {
				try {
					URI u = new URI("file://" + fileName + "/" + i + "_" + threadNum + ".txt");
					File f = new File(u.getPath());
					if (f.exists() == false) {
						f.createNewFile();
					}
					fis = new FileInputStream(f);
					openFiles++;
					if (openFiles > maxOpenFiles) {
						maxOpenFiles = openFiles;
					}
					FileDescriptor fd = fis.getFD();
					if (fstatFlag) {
						//synchronized (sync) {
							Stat stat = NativeIO.fstat(fd);
							nothing += stat.getGroup().length() + stat.getOwner().length();
							
						//}
					}
					Thread.sleep((int)(waitTime ));
				} catch (Exception e) {
					System.err.println("It failed at " +i  + " FCnt:" + ++FStatTest.failures + " OF:" + openFiles);
					e.printStackTrace();
				}	finally
				{
					if (fis != null) {
						try {
							fis.close();
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						openFiles--;
					}
				}
			}
			System.out.println("Finished:S:" + ++FStatTest.successes + " F:" + FStatTest.failures + " " + nothing + " max:" + maxOpenFiles + " currentOpen:" + openFiles);
		}
	}
}
