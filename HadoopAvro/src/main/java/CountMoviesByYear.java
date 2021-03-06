import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountMoviesByYear extends Configured  implements Tool{

	public static void main(String[] args) throws Exception  {
		
		int exitCode=ToolRunner.run(new CountMoviesByYear(), args);
		System.out.println("Exit code "+exitCode);    

	}

	public int run(String[] args) throws Exception {
		Job job=Job.getInstance(getConf(),"CountMoviesByYear");
		job.setJarByClass(getClass());
		job.setMapperClass(MovieCounterMapper.class);
		job.setReducerClass(MovieCounterReducer.class);
		
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
		return job.waitForCompletion(true) ? 0:1;
	}

	public static class MovieCounterMapper extends Mapper<AvroKey<GenericRecord>,NullWritable,AvroKey<String> ,AvroValue<Integer> >{
		
		public void map(AvroKey<GenericRecord> key,NullWritable value,Context context) throws IOException,InterruptedException{
		  context.write(new AvroKey(key.datum().get("year")), new AvroValue(new Integer(1)));	
		}
	}
	
	public static class MovieCounterReducer extends Reducer<AvroKey<String> ,AvroValue<Integer>,Text,IntWritable>{
		@Override
		public void reduce (AvroKey<String> key,Iterable<AvroValue<Integer> > values,Context context) throws IOException,InterruptedException{
			int count=0;
			for(AvroValue<Integer> value:values){
				count+=value.datum().intValue();
			}
			context.write(new Text(key.datum().toString()), new IntWritable(count));
		}
	}
}
