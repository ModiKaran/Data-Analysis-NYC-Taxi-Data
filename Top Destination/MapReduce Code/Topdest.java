import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Topdest {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line,",");
        int i =0;
        Text words[] = new Text[14];
        while (tokenizer.hasMoreTokens()) {
            words[i]=new Text(tokenizer.nextToken());
            System.out.println(i+":"+words[i]);
            i++;
        }
        System.out.println(i);
        if(i==14){
            context.write(new Text(words[12]+" "+words[13]), new IntWritable(1));
        }
        else{
            context.write(new Text(words[11]+" "+words[12]), new IntWritable(1));
        }
        }
    }
 
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += 1;
        }
        context.write(key, new IntWritable(sum));
    }
 }

 public static class Mapnext extends Mapper<LongWritable, Text, IntWritable, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String values[] = line.split("\t");
        context.write(new IntWritable(Integer.parseInt(values[1])), new Text(values[0]));
    }
 } 

 public static class Reducenext extends Reducer<IntWritable, Text, Text, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        for (Text val : values) {
            context.write(val, key);
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "topdest");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Topdest.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    Configuration conf2 = new Configuration();
    
    Job nextjob = new Job(conf2, "topdest");
    
    nextjob.setOutputKeyClass(IntWritable.class);
    nextjob.setOutputValueClass(Text.class);
        
    nextjob.setMapperClass(Mapnext.class);
    nextjob.setReducerClass(Reducenext.class);
        
    nextjob.setInputFormatClass(TextInputFormat.class);
    nextjob.setOutputFormatClass(TextOutputFormat.class);

    nextjob.setNumReduceTasks(1);
    nextjob.setJarByClass(Topdest.class);

    FileInputFormat.addInputPath(nextjob, new Path(args[1]));
    FileOutputFormat.setOutputPath(nextjob, new Path(args[2]));

    job.submit();
    if(job.waitForCompletion(true)) {
        nextjob.submit();
        nextjob.waitForCompletion(true);
    }
        
  }
        
}
