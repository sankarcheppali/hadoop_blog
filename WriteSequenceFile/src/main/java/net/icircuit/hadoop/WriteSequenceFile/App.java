package net.icircuit.hadoop.WriteSequenceFile;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	String srcUri = args[0];
        String destUri=args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Text key=new Text();
        TextArrayWritable value=new TextArrayWritable();
        SequenceFile.Writer seqWriter = null;
        InputStream is=fs.open(new Path(srcUri));
        
        BufferedReader br=new BufferedReader(new InputStreamReader(is,"UTF-8"));
        try {
          seqWriter = new SequenceFile.Writer(fs, conf, new Path(destUri),
          key.getClass(),value.getClass());
          //read from br and split the string,then write to seqWriter
          String line;
          while(( line=br.readLine())!=null){
        	  key.set(line.split("\t")[0]);
        	  String[] values=line.split("\t")[1].split(",");
        	  Writable[] witable=new Text[values.length];
        	  for(int i=0;i<values.length;i++){
        		  witable[i]=new Text(values[i]);
        	  }
        	  value.set(witable);
        	  seqWriter.append(key, value);
          }
        } finally {
          IOUtils.closeStream(seqWriter);
        }
    }
}
