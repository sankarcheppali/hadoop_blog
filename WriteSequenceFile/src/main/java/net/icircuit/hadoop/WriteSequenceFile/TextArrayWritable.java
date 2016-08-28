package net.icircuit.hadoop.WriteSequenceFile;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextArrayWritable extends ArrayWritable{
	
	public  TextArrayWritable() {
		super(Text.class);
	}
	
   public String toString(){
	  String str="";
	  for(Writable text:this.get()){
		  str+=text.toString()+",";
	  }
	   return    str;
   }
	
}
