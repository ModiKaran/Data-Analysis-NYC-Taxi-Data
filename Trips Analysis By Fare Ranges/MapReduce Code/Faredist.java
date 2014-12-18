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
        
public class Faredist {

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> { 
   
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
        float f =0;
	try{
            if((foo[5].trim()).equals("fare_amount")){
                System.out.println("NA");
            }
            else{
                f = Float.parseFloat(foo[10].toString());
                if(f<10.0){
	        context.write(new Text("1"),new IntWritable(1));
                }
                else if(f>=10.0 && f<20.0){
	        context.write(new Text("2"),new IntWritable(1));
                }
                else if(f>=20.0 && f<30.0){
	        context.write(new Text("3"),new IntWritable(1));
                }
                else if(f>=30.0 && f<40.0){
	        context.write(new Text("4"),new IntWritable(1));
                }
                else if(f>=40.0 && f<50.0){
	        context.write(new Text("5"),new IntWritable(1));
                }
                else if(f>=50.0 && f<60.0){
	        context.write(new Text("6"),new IntWritable(1));
                }
                else if(f>=60.0 && f<70.0){
	        context.write(new Text("7"),new IntWritable(1));
                }
                else if(f>=70.0 && f<80.0){
	        context.write(new Text("8"),new IntWritable(1));
                }
                else if(f>=80.0 && f<90.0){
	        context.write(new Text("9"),new IntWritable(1));
                }
                else if(f>=90.0 && f<100.0){
	        context.write(new Text("10"),new IntWritable(1));
                }
                else{
	        context.write(new Text("11"),new IntWritable(1));
                }
            }
	}
	catch(Exception e){
		//System.out.println("Shit is"+Arrays.toString(foo));
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
        
        Job job = new Job(conf, "faredist");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Faredist.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

  }
        
}
