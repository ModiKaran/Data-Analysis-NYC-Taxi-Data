import java.io.IOException;
import java.util.*;
import java.io.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Distribution {
 static int i=0;
 static float total=0;

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> { 
   
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
	try{
            if((foo[5].trim()).equals("fare_amount")){
                System.out.println("NA");
            }
            else{
	        context.write(new Text(foo[5]), new IntWritable(1));
	        context.write(new Text(foo[6]), new IntWritable(2));
	        context.write(new Text(foo[7]), new IntWritable(3));
	        context.write(new Text(foo[8]), new IntWritable(4));
	        context.write(new Text(foo[9]), new IntWritable(5));
            }
	}
	catch(Exception e){
		//System.out.println("Shit is"+Arrays.toString(foo));
	}
    }
 } 

 public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        for (IntWritable val : values){
            context.write(new Text(val+""), new FloatWritable(Float.parseFloat(key.toString())));
      }
     }
  }


 public static class Mapnext extends Mapper<LongWritable, Text, IntWritable, Text> {
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String values[] = line.split("\t");
        context.write(new IntWritable(Integer.parseInt(values[0])), new Text(values[1]));
    }
 } 

 public static class Reducenext extends Reducer<IntWritable, Text, Text, FloatWritable> {
    float sum =0;
    int j = 6;
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        try{
            for (Text val : values) {
                sum += Float.parseFloat(val.toString());
            }
            context.write(new Text(key+""), new FloatWritable(sum));
            i +=1;
            total +=sum;
            sum =0;
            if(i>4){
            context.write(new Text(j+""), new FloatWritable(total));          
            }
        }
	catch(Exception e){
		//System.out.println("Shit is"+Arrays.toString(foo));
	}
      }
  }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "distribution");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Distribution.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    Configuration conf2 = new Configuration();
    
    Job nextjob = new Job(conf2, "distribution");
    
    nextjob.setOutputKeyClass(IntWritable.class);
    nextjob.setOutputValueClass(Text.class);
        
    nextjob.setMapperClass(Mapnext.class);
    nextjob.setReducerClass(Reducenext.class);
        
    nextjob.setInputFormatClass(TextInputFormat.class);
    nextjob.setOutputFormatClass(TextOutputFormat.class);

    nextjob.setNumReduceTasks(1);
    nextjob.setJarByClass(Distribution.class);

    FileInputFormat.addInputPath(nextjob, new Path(args[1]));
    FileOutputFormat.setOutputPath(nextjob, new Path(args[2]));

    job.submit();
    if(job.waitForCompletion(true)) {
        nextjob.submit();
        nextjob.waitForCompletion(true);
    }

  }
        
}
