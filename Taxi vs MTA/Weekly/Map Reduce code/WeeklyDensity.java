import java.io.IOException;
import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WeeklyDensity {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static Date[] checkpoints = new Date[53];
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void setup(Context context) throws IllegalArgumentException, IOException {
		try{
			Calendar cal = Calendar.getInstance();
			checkpoints[0] = formatter.parse("2013-01-04");
			for(int i=1;i<53;i++){
				cal.setTime(checkpoints[i-1]);
				cal.add(Calendar.DATE, 7);
				checkpoints[i] = cal.getTime();
			}
		}
		catch(Exception e){
		}
    }
   		
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
	try{

		Date mydate = formatter.parse(foo[5]);
		for(int i=0;i<checkpoints.length;i++){
			if(mydate.before(checkpoints[i])){
				context.write(new Text(new SimpleDateFormat("yyyy-MM-dd").format(checkpoints[i])), new IntWritable(1));
				break;
			}
		}
	}
	catch(Exception e){
		//Pass
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
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "WeeklyDensity");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WeeklyDensity.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
