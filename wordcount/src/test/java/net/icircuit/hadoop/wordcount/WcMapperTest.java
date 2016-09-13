package net.icircuit.hadoop.wordcount;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.apache.hadoop.mrunit.types.Pair;
public class WcMapperTest {
     
	@SuppressWarnings("unchecked")
	@Test
	public void mapperBreakesTheRecord() throws IOException {
		new MapDriver<LongWritable,Text,Text,IntWritable>()
		.withMapper(new WcMapper())
		.withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
		.withAllOutput(Arrays.asList(
				new Pair<Text,IntWritable>(new Text("msg1"),new IntWritable(1)),
				new Pair<Text,IntWritable>(new Text("msg2"),new IntWritable(1)),
				new Pair<Text,IntWritable>(new Text("msg1"),new IntWritable(1))
				))
		.runTest();
	}
	
	@Test
	public void testSumReducer() throws IOException {
	 new ReduceDriver<Text,IntWritable,Text,IntWritable>()
	 .withReducer(new WcReducer())
	 .withInput(new Text("msg1"), Arrays.asList(new IntWritable(1),new IntWritable(1)))
	 .withOutput(new Text("msg1"), new IntWritable(2))
	 .runTest();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testWordCount() throws IOException{
		new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>()
		.withMapper(new WcMapper())
		.withReducer(new WcReducer())
		.withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
		.withAllOutput(Arrays.asList(
				new Pair<Text,IntWritable>(new Text("msg1"),new IntWritable(2)),
				new Pair<Text,IntWritable>(new Text("msg2"),new IntWritable(1))
				))
		.runTest();
	}
   

}
