package net.icircuit.hadoop.HDFSRead;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		InputStream is=fs.open(new Path(args[0]));
		while(is.available()>0){
			System.out.print((char)is.read());
		}
		is.close();
	}

}
