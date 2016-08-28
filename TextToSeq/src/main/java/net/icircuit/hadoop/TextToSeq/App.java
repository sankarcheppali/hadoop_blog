package net.icircuit.hadoop.TextToSeq;


import java.util.ArrayList;
import net.icircuit.hadoop.TextToSeq.JobBuilder;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "JOb Started" );
        System.out.println("Exit coode "+ToolRunner.run(new App(),args));
    }



	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
        Job job=Job.getInstance();
		job.setJobName("SingerSong");
		job.setJarByClass(App.class);
		job.setMapperClass(SingerSongMapper.class);
		job.setReducerClass(SingerSongReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int ecode=job.waitForCompletion(true) ? 0:1;
		return ecode;
				
	}
}
