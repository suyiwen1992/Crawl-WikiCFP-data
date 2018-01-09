import java.io.IOException;
import java.util.StringTokenizer;
  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount{

   public static class TokenizerMapper
   extends Mapper<Object, Text, Text, Text>{
         // private final static IntWritable one = new IntWritable(1);
          private Text tem = new Text();
          private Text word = new Text();
          public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
                // StringTokenizer itr = new StringTokenizer (value.toString(),"\t");
                // while(itr.hasMoreTokens()){
                   // word.set(itr.nextToken().nextToken().nextToken());
                    String[] str=value.toString().split(";"); //split each line by ";"
                   if(str.length>2){
                    String[] str1=str[2].split(",");          //Only store the city name and throw the contry and state name.
                    word.set(str1[0]);
                    tem.set(str[0]);
                    context.write(word,tem);}
                 }
          }


public static class IntSumReducer extends Reducer<Text,Text, Text, Text>{
                private Text res= new Text();
          public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
InterruptedException{
            //    int count=0;
                String tem = new String();
                for (Text val : values){
            //       count++;
                   tem+=val.toString()+";";
                }
                Text te= new Text(tem);
                res.set(te);
                context.write(key,res);
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
          job.setOutputValueClass(Text.class);
          FileInputFormat.addInputPath(job,new Path(args[0]));
          FileOutputFormat.setOutputPath(job,new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0:1);


  }


}
