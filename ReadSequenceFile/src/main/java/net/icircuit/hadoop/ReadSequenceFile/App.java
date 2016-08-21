package net.icircuit.hadoop.ReadSequenceFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IllegalArgumentException, IOException
    {
    	 String uri = args[0];
    	    Configuration conf = new Configuration();
    	    FileSystem fs = FileSystem.get( conf);
    	    SequenceFile.Reader reader = null;
    	    try {
    	      reader = new SequenceFile.Reader(fs, new Path(uri), conf);
    	      Writable key = (Writable)
    	          ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    	      Writable value = (Writable)
    	          ReflectionUtils.newInstance(reader.getValueClass(), conf);
    	      while (reader.next(key, value)) {
    	        System.out.println("Key:"+key+",Value:"+value);
    	      }
    	    } finally {
    	      IOUtils.closeStream(reader);
    	    }
    }
}
