import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.Op.Create;

import java.io.IOException;

import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;


public class MRTextToAvro  extends Configured implements Tool{	
	public static void main(String[] args) throws Exception {
     int exitCode=ToolRunner.run(new MRTextToAvro(),args );
     System.out.println("Exit code "+exitCode);    
	}

	public int run(String[] arg0) throws Exception {
		Job job= Job.getInstance(getConf(),"Text To Avro");
		
		job.setJarByClass(getClass());
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		Schema.Parser parser = new Schema.Parser();
		Schema schema=parser.parse(getClass().getResourceAsStream("movies.avsc"));
		
		AvroJob.setMapOutputKeySchema(job,schema);
		job.setMapOutputValueClass( NullWritable.class);
		AvroJob.setOutputKeySchema(job, schema);
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(AvroKeyOutputFormat.class);
	    
	    job.setMapperClass(TextToAvroMapper.class);
	    job.setReducerClass(TextToAvroReduce.class);

		return job.waitForCompletion(true)?0:1;
	}

	public static class TextToAvroMapper extends Mapper<LongWritable ,Text,AvroKey<GenericRecord>,NullWritable>{
		Schema schema;
	    
		  protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Schema.Parser parser = new Schema.Parser();
            schema=parser.parse(getClass().getResourceAsStream("movies.avsc"));
		  }

	   public void map(LongWritable  key,Text value,Context context) throws IOException,InterruptedException{
		   GenericRecord record=new GenericData.Record(schema);
		   String inputRecord=value.toString();
		   
		   record.put("movieName", getMovieName(inputRecord));
		   record.put("year", getMovieRelaseYear(inputRecord));
		   record.put("tags", getMovieTags(inputRecord));
		   context.write(new AvroKey(record), NullWritable.get());
	   }	
	   public String getMovieName(String record){
		   String movieName=record.split("::")[1];
		   return movieName.substring(0, movieName.lastIndexOf('(' )).trim();
	   }
	   public String getMovieRelaseYear(String record){
		   String movieName=record.split("::")[1];
		   return movieName.substring( movieName.lastIndexOf( '(' )+1,movieName.lastIndexOf( ')' )).trim();
	   }
	   public String[] getMovieTags(String record){
		   return (record.split("::")[2]).split("\\|");
	   }
	}

	public static class TextToAvroReduce extends Reducer<AvroKey<GenericRecord>,NullWritable,AvroKey<GenericRecord>,NullWritable>{
		@Override
		public void reduce(AvroKey<GenericRecord> key,Iterable<NullWritable> value,Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
}
