//package org.apache.hadoop.wordcount;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.map.InverseMapper;


public class WordCount{

   public static class TokenizerMapper  //Mapping the data 
   extends Mapper<Object, Text, Text, IntWritable>{
          private final static IntWritable one = new IntWritable(1);
          private Text word = new Text();
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                // StringTokenizer itr = new StringTokenizer (value.toString(),"\t");
                // while(itr.hasMoreTokens()){
                   // word.set(itr.nextToken().nextToken().nextToken());
                    String[] str=value.toString().split(";");
                   if(str.length>2){
                    String[] str1=str[2].split(",");
                    word.set(str1[0]);
                    context.write(word,one);}
                 }
          }

/*public static class InverseMapper extends Mapper<Text,IntWritable,IntWritable,Text> {  


          // The inverse function.  Input keys and values are swapped.
          
          public void map(Text key, IntWritable value, Context context  
                          ) throws IOException, InterruptedException {  
            context.write(value, key);  
          }
      }*/

public static class IntSumReducer extends Reducer<Text,IntWritable, Text, IntWritable>{ //Reduce process
          private IntWritable result=new IntWritable();
          public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
InterruptedException{
                int sum=0;
                for (IntWritable val : values){
                   sum+=val.get();
                }
                result.set(sum);
                context.write(key,result);
          }
    }

/*public static class InverseReducer extends Reducer<IntWritable,Text, IntWritable, Text>{
            private Text result=new Text();
            public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
  InterruptedException{
                  String sum = new String();
                  for (Text val : values){
                     sum+=val.toString()+";";
                  }
                  result.set(sum);
                  context.write(key,result);
            }
      }*/


public static void main(String[] args) throws Exception{
          Configuration conf=new Configuration();
//          Path tempDir=new Path("wordcount9-temp-output");
          Job job=Job.getInstance(conf, "word count");
          job.setJarByClass(WordCount.class);
          job.setMapperClass(TokenizerMapper.class);
          job.setCombinerClass(IntSumReducer.class);
          job.setReducerClass(IntSumReducer.class);
//          job.setOutputFormat(SequenceFileOutputFormat.class); 
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
          FileInputFormat.addInputPath(job,new Path(args[0]));
//          job.setOutputFormatClass(TextOutputFormat.class);
          FileOutputFormat.setOutputPath(job,new Path(args[1]));
          job.waitForCompletion(true);
      /*
          Configuration conf1=new Configuration();
          Job sortjob = Job.getInstance(conf1,"sort");
          sortjob.setJarByClass(WordCount.class);
//          sortjob.setInputFormatClass(KeyValueTextInputFormat.class);
          sortjob.setInputFormatClass(SequenceFileInputFormat.class);
          sortjob.setInputKeyClass(Text.class);
          sortjob.setInputValueClass(IntWritable.class);
          sortjob.setMapperClass(InverseMapper.class);
          sortjob.setCombinerClass(InverseReducer.class);
          sortjob.setReducerClass(InverseReducer.class);
          sortjob.setOutputKeyClass(Text.class);
          sortjob.setOutputValueClass(IntWritable.class);
//          job.setInputFormatClass(KeyValueTextInputFormat);
          FileInputFormat.addInputPath(sortjob,tempDir);
          FileOutputFormat.setOutputPath(sortjob,new Path(args[1]));
//          job.setInputFormatClass(SequenceFileInputFormat.class);
          sortjob.waitForCompletion(true);
          FileSystem.get(conf1).delete(tempDir,true);*/
          System.exit(0);


  }


}
