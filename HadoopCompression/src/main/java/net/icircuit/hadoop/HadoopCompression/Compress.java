package net.icircuit.hadoop.HadoopCompression;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class Compress {

	public static void main(String[] args) throws ClassNotFoundException, IOException{ //will take compression codec as argumnet
		Class<?> codeClass=Class.forName(args[0]);
		Configuration conf = new Configuration();
		CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(codeClass, conf);
		 CompressionOutputStream out = codec.createOutputStream(System.out);
		   IOUtils.copyBytes(System.in, out, 4096, false);
		    out.finish();//will not close the underlying output stream
	}
}
