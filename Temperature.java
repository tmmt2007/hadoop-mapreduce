package org.myorg;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import  org.apache.hadoop.mapreduce.Job;
import  org.apache.hadoop.mapreduce.Mapper;
import  org.apache.hadoop.mapreduce.Reducer;
import  org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import  org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Temperature {

  static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
      // print: Before Mapper: 0, 2000010115
      System.out.print("Before Mapper: " + key + ", " + value);
      String line = value.toString();
      String year = line.substring(0, 4);
      int temperature = Integer.parseInt(line.substring(8));
      context.write(new Text(year), new IntWritable(temperature));
      // print: After Mapper: 2000, 15
      System.out.println(
                 "======" +
                 "After Mapper: " + new Text(year) + ", " + new IntWritable(temperature));
    }
  }

  static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
                       throws IOException, InterruptedException {
      int maxValue = Integer.MIN_VALUE;
      StringBuffer sb = new StringBuffer();
      // get values maxmium
      for(IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
        sb.append(value).append(", ");
      }
      // print: Before Reduce: 2000, 15, 23, 99, 12, 22
     System.out.print("Before Reduce: " + key + ", " + sb.toString());
     context.write(key, new IntWritable(maxValue));
     // print: After Reduce: 2000, 99
     System.out.println(
                "======" +
                "After Reduce: " + key + ", " + maxValue);
    }
  }

  public static void main(String[] args) throws Exception {
    // input path
    String dst = "hdfs://discovery3:9000/tmp/flyingsky2007/test1/input.txt";
    // output path
    String dstOut = "hdfs://discovery3:9000/tmp/flyingsky2007/test1/output";
    Configuration hadoopConfig = new Configuration();

    hadoopConfig.set("fs.hdfs.impl",
         org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    hadoopConfig.set("fs.file.impl",
         org.apache.hadoop.fs.LocalFileSystem.class.getName());
    Job job = new Job(hadoopConfig);

    //job.setJarByClass(NewMaxTemperature.class);

    // set the path of input and output
    FileInputFormat.addInputPath(job, new Path(dst));
    FileOutputFormat.setOutputPath(job, new Path(dstOut));

    // 
    job.setMapperClass(TempMapper.class);
    job.setReducerClass(TempReducer.class);

    // set the output types of Key and Value
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // run the job
    job.waitForCompletion(true);
    System.out.println("Finished");

  }

}



