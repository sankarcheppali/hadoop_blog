package net.icircuit.hadoop.HadoopCompression;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class Decompress {
	public static void main(String[] args) throws ClassNotFoundException, IOException{ //will take compression codec as argumnet
		Class<?> codeClass=Class.forName(args[0]);
		Configuration conf = new Configuration();
		CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(codeClass, conf);
		 CompressionInputStream in = codec.createInputStream(System.in);
		   IOUtils.copyBytes(in,System.out, 4096, false);
		    in.close();
	}
}
