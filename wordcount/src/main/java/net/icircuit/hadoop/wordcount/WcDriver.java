package net.icircuit.hadoop.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WcDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
	
      int exitCode=ToolRunner.run(new WcDriver(), args);
      System.out.println("Exit code :"+exitCode);
      System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		Job job=Job.getInstance();
		
		job.setJobName("Word Count");
		job.setJarByClass(WcDriver.class);
		
		job.setMapperClass(WcMapper.class);
		job.setReducerClass(WcReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		int ecode=job.waitForCompletion(true) ? 0:1;
		
		return ecode;
		
	}

}
