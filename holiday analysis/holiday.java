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
        
public class holiday {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    Calendar cal = new GregorianCalendar();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
	// This program performs national holiday analysis of taxi density.
	
	try{ 

		String[] line = value.toString().split(",");	
		String date = line[3].substring(0,10);	
		int year = Integer.parseInt(line[3].substring(0,4));
		int month = Integer.parseInt(line[3].substring(5,7));
		int day = Integer.parseInt(line[3].substring(8,10));
		
		if(month==1 && day ==1){		
			context.write(new Text("New Year's Day"), one);
		}

		else if(month==1 &&day ==21){		
			context.write(new Text("Martin King Luther's Birthday"), one);
		}

		else if(month==2 &&day ==18){		
			context.write(new Text("President's Day"), one);
		}
		else if(month==5 &&day ==27){		
			context.write(new Text("Memorial Day"), one);
		}
		else if(month==7 &&day ==4){		
			context.write(new Text("July 4th"), one);
		}
		else if(month==9 &&day ==2){		
			context.write(new Text("Labour's Day"), one);
		}
		else if(month==10 &&day ==14){		
			context.write(new Text("Columbus Day"), one);
		}
		else if(month==11 &&day ==11){		
			context.write(new Text("Veterans Day"), one);
		}
		else if(month==11 &&day ==28){		
			context.write(new Text("Thanksgiving!"), one);
		}
		else if(month==12 &&day ==25){		
			context.write(new Text("Christmas Day"), one);
		}
	
		else if(month==11 &&day ==8){		
			context.write(new Text("Shashank's Day"), one);
		}
		else if(month==11 &&day ==8){		
			context.write(new Text("Sanmeet's Day"), one);
		}
		else if(month==1 &&day ==11){		
			context.write(new Text("Karan's Day"), one);
		}
		else{
			context.write(new Text("Normal Day"), one);		
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
        
        Job job = new Job(conf, "holiday");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(holiday.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
