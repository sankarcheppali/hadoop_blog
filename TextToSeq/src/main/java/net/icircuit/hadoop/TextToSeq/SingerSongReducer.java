package net.icircuit.hadoop.TextToSeq;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
public class SingerSongReducer extends Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
		ArrayList<Text> vs=new ArrayList<Text>();
		for(Text t:values){
			vs.add(t);
		}
		
		context.write(key, new Text(vs.toString()));
	}
}
