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
        
public class Tips1 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
	try{
		float amount = Float.parseFloat(foo[8]);
		int month = Integer.parseInt(foo[3].substring(5,7));
		if((foo[4].equals("CSH") || foo[4].equals("CRD")) && amount!=0 && month>=1 && month<=12){
			if(foo[4].equals("CSH")){
				context.write(new Text(foo[3].substring(5,7)+"0"), new FloatWritable(amount));
			}
			else if(foo[4].equals("CRD")){
				context.write(new Text(foo[3].substring(5,7)+"1"), new FloatWritable(amount));
			}
		}
	}
	catch(Exception e){
		//Pass
	}
    }
 } 
        
 public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) 
      throws IOException, InterruptedException {
        float sum = 0;
	int count = 0;
        for (FloatWritable val : values) {
        	sum += val.get();
		count += 1;
        }
        context.write(key, new FloatWritable((float)sum/(float)count));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "tips1");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Tips1.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
