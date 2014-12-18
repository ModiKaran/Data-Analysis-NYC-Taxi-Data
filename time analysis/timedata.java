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
        
public class timedata {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
         
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
	// This program performs morning, noon, evening, night and late night analysis of taxi density.
	
	try{ 

		String[] line = value.toString().split(",");	
		String data = line[3];		
		int time_hour  = Integer.parseInt(data.substring(11,13));
		int time_min = Integer.parseInt(data.substring(14,16));
		
		// morning = 6 am to 12 pm; noon = 12 to 4pm, evening= 4pm to 7 pm; night= 7pm to 12am; late night=12am to 6am
		
		if ( (6<=time_hour && time_hour<=11) && (0<=time_min && time_min<=59) ){
			context.write(new Text("Morning"), one);
		}
		
		else if ( (12<=time_hour && time_hour<=15) && (0<=time_min && time_min <=59) ){
			context.write(new Text("Noon"), one);		
		}
		
		else if ( (16<=time_hour && time_hour<=18) && (0<=time_min && time_min <=59) ){
			context.write(new Text("Evening"), one);		
		}

		else if ( (19<=time_hour && time_hour <=23) && (0<=time_min && time_min <=59) ){
			context.write(new Text("Night"), one);		
		}

		else if ( (0<=time_hour && time_hour <=5) && (0<=time_min && time_min <=59) ){
			context.write(new Text("Late Night"), one);		
		}

		else {
			context.write(new Text("Missing some values"), one);		
		}
	
        }
	catch (Exception e){
		// pass
	}
	
	}
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "timedata");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(timedata.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
