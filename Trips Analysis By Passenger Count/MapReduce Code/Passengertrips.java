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
        
public class Passengertrips {

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> { 
   
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	try{
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            int i =0;
            int f =0;
            Text foo[] = new Text[14];
            while (tokenizer.hasMoreTokens()) {
                foo[f]=new Text(tokenizer.nextToken());
                //System.out.println(f+":"+words[f]);
                f++;
            }

            if(f==14){
                i = Integer.parseInt(foo[7].toString());
            }
            else{
                i = Integer.parseInt(foo[6].toString());
            }
            
            if(i==1){
	        context.write(new Text("1"),new IntWritable(1));
            }
            else if(i==2){
	        context.write(new Text("2"),new IntWritable(1));
            }
            else if(i==3){
	        context.write(new Text("3"),new IntWritable(1));
            }
            else if(i==4){
	        context.write(new Text("4"),new IntWritable(1));
            }
            else if(i==5){
	        context.write(new Text("5"),new IntWritable(1));
            }
            else if(i==6){
	        context.write(new Text("6"),new IntWritable(1));
            }
            else{
	        context.write(new Text(">6"),new IntWritable(1));
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
        
        Job job = new Job(conf, "passengertrips");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Passengertrips.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

  }
        
}
