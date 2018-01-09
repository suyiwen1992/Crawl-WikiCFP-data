import java.io.IOException;
import java.util.StringTokenizer;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount{

public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntArrayWritable>{
          private Text word = new Text();
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                    IntWritable[] tem = new IntWritable[8];
                    for (int i = 0; i < 8; i++) tem[i] = new IntWritable(0);
                    String[] str=value.toString().split(";");
                   if(str.length>2){                            //map conference year of the city
                         String[] str1=str[2].split(",");
                         if(str[0].contains("2011")) tem[0]=new IntWritable(1);
                         if(str[0].contains("2012")) tem[1]=new IntWritable(1);
                         if(str[0].contains("2013")) tem[2]=new IntWritable(1);
                         if(str[0].contains("2014")) tem[3]=new IntWritable(1);
                         if(str[0].contains("2015")) tem[4]=new IntWritable(1);
                         if(str[0].contains("2016")) tem[5]=new IntWritable(1);
                         if(str[0].contains("2017")) tem[6]=new IntWritable(1);
                         if(str[0].contains("2018")) tem[7]=new IntWritable(1);
                         IntArrayWritable res = new IntArrayWritable();
                         word.set(str1[0]);
                         res.set(tem);
                         context.write(word,res);
                    }
                 }
          }


public static class IntSumReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable>{   //reduce the conferene year array of each city
        //  private Text res = new Text();      
          public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException,
InterruptedException{
                IntWritable[] sum = new IntWritable[8];
                for(int i=0;i<8;i++) sum[i]=new IntWritable(0);
                for (IntArrayWritable val : values){
                     IntWritable[] tem1 = new IntWritable[8];
                     for(int i=0;i<8;i++){
                         tem1[i]=(IntWritable) val.get()[i]; 
                     }
                     for(int i=0;i<8;i++){
                        sum[i] = new IntWritable (sum[i].get()+tem1[i].get());
                     }
                    
                    }
                 
                IntArrayWritable res = new IntArrayWritable();
                res.set(sum);
                context.write(key,res);
          }
    }

     public static class IntArrayWritable extends ArrayWritable {
          public IntArrayWritable(IntWritable[] intWritables) {
             super(IntWritable.class);
          }
          public IntArrayWritable() {
              super(IntWritable.class);
         }

         @Override
         public String toString() {           //override the toString to make the output be readable
             StringBuilder sb = new StringBuilder();
             for (String s : super.toStrings())
             {
                 sb.append(s).append(" ");
             }
             return sb.toString();
         }

     }


public static void main(String[] args) throws Exception{
          Configuration conf=new Configuration();
          Job job=Job.getInstance(conf, "word count");
          job.setJarByClass(WordCount.class);
          job.setMapperClass(TokenizerMapper.class);
          job.setCombinerClass(IntSumReducer.class);
          job.setReducerClass(IntSumReducer.class);
          job.setOutputKeyClass(Text.class);
        //  job.setMapOutputValueClass(Text.class);
          job.setOutputValueClass(IntArrayWritable.class);
          FileInputFormat.addInputPath(job,new Path(args[0]));
          FileOutputFormat.setOutputPath(job,new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0:1);


  }


}
