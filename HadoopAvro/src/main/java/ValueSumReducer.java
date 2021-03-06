import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ValueSumReducer extends Reducer<AvroKey<String>,AvroValue<Integer>,Text,IntWritable> {
	@Override
	public void reduce (AvroKey<String> key,Iterable<AvroValue<Integer> > values,Context context) throws IOException,InterruptedException{
		int count=0;
		for(AvroValue<Integer> value:values){
			count+=value.datum().intValue();
		}
		context.write(new Text(key.datum().toString()), new IntWritable(count));
	}
}
