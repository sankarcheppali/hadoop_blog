package net.icircuit.hadoop.HDFSWrite;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class Main extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode=ToolRunner.run(new Main(), args);
		System.out.println("Exit code "+exitCode);

	}

	public int run(String[] args) throws Exception {
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);

		OutputStream os=fs.create(new Path(args[1]));
		InputStream is=new FileInputStream(args[0]);

		IOUtils.copyBytes(is, os, 4096, true);		
		return 0;
	}

}
