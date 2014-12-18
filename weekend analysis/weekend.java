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
        
public class weekend {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    Calendar cal = new GregorianCalendar();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
	// This program performs weekday vs weeekend analysis for taxi density.
	
	try{ 

		String[] line = value.toString().split(",");	
		String date = line[3].substring(0,10);	
		int year = Integer.parseInt(line[3].substring(0,4));
		int month = Integer.parseInt(line[3].substring(5,7));
		int day = Integer.parseInt(line[3].substring(8,10));
		cal.set(year,month-1,day);					// month is 0-index based. So --> -1
		//String week_day =""+cal.get(Calendar.DAY_OF_WEEK);	
		int week_day = cal.get(Calendar.DAY_OF_WEEK);      	
		
		if(week_day == 1 || week_day==7){		//weekend
		
			context.write(new Text("Weekend"), one);
		}
		else{
			context.write(new Text("Weekday"), one);
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
        
        Job job = new Job(conf, "weekend");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(weekend.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
