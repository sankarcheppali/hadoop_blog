import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class CountMovieTagsDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		int exitCode=ToolRunner.run(new CountMovieTagsDriver(), args);
		System.out.println("Exit code "+exitCode);
	}
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf(), "Count Movie tags");
		
		job.setJarByClass(getClass());
		job.setMapperClass(CountMovieTagsMapper.class);
		job.setReducerClass(ValueSumReducer.class);

		Schema.Parser parser = new Schema.Parser();
		Schema schema=parser.parse(getClass().getResourceAsStream("movies.avsc"));
		AvroJob.setInputKeySchema(job, schema);
		
		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.INT));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}

}
