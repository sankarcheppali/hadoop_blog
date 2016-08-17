package net.icircuit.spark.RDDCreation;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App 
{
    public static void main( String[] args )
    {
        SparkConf conf=new SparkConf().setAppName("RDDCreationExample");
        JavaSparkContext sc=new JavaSparkContext(conf);
       
        //create RDD from an array
        JavaRDD<String> parNames=sc.parallelize(Arrays.asList("sankar","anshu"));
        System.out.println("Loading text file from "+args[0]);
        JavaRDD<String> namesFromFile=sc.textFile(args[0]);
        
        System.out.println("parNames Count "+parNames.count());
        System.out.println("namesFromFile Count "+namesFromFile.count());
        //close the context ?
        sc.close();
    }
}
