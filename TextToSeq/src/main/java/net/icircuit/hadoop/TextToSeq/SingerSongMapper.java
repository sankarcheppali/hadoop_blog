package net.icircuit.hadoop.TextToSeq;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SingerSongMapper extends Mapper<LongWritable,Text,Text,Text>{

	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		//we don't want the first record
		
		if(key.get()==0){
			return;
		}
		//simple split, does not check for the , inside the tokens, use a lib for production use
		String[] tokens=value.toString().split(",");
		context.write(new Text(tokens[4]), new Text(tokens[5]));
	}

}
