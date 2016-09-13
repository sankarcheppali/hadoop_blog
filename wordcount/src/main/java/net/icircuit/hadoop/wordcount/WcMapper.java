package net.icircuit.hadoop.wordcount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class WcMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	public void map(LongWritable key,Text value,Context context) throws
	IOException,InterruptedException{
		String line=value.toString();
		String[] words=line.split(" ");
		System.out.println("Length of "+value.toString()+" ,"+words.length );
		for(String word:words){
			context.write(new Text(word),new IntWritable(1));
		}
	}
}
