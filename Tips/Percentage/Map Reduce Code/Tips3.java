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
        
public class Tips3 {
 static int cash_amt =0;
 static int card_amt =0;
 static int cash_cnt =0;       
 static int card_cnt =0;
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
	try{
		int amount = Integer.parseInt(foo[8]);
		int month = Integer.parseInt(foo[3].substring(5,7));
		if((foo[4].equals("CSH") || foo[4].equals("CRD")) && month>=1 && month<=12){
			if(foo[4].equals("CSH")){
                                cash_cnt += 1;
			}
			else if(foo[4].equals("CRD")){
                                card_cnt += 1;
			}
		}
		if((foo[4].equals("CSH") || foo[4].equals("CRD")) && amount!=0 && month>=1 && month<=12){
			if(foo[4].equals("CSH")){
				context.write(new Text(foo[3].substring(5,7)+"0"), new IntWritable(amount));
                                cash_amt += amount;
			}
			else if(foo[4].equals("CRD")){
				context.write(new Text(foo[3].substring(5,7)+"1"), new IntWritable(amount));
                                card_amt += amount;
			}
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
        int sum = 0;
	int count = 0;
        int total_tip = 0;
        for (IntWritable val : values) {
        	sum += val.get();
		count += 1;
        }
        total_tip = card_amt + cash_amt;
        context.write(key, new FloatWritable((float)sum/(float)count));
        if (key.toString().substring(key.toString().length() - 1).equals("0")){
          // % of total tips contributed by cash 
          context.write(key, new FloatWritable((float)cash_amt));
          context.write(key, new FloatWritable((float)(cash_amt*100)/(float)total_tip));
          // % of cash type paying tips
          context.write(key, new FloatWritable((float)(100*count)/(float)cash_cnt));
          context.write(key, new FloatWritable((float)count));
          context.write(key, new FloatWritable((float)cash_cnt));
        }
        else{
          // % of total tips contributed by card
          context.write(key, new FloatWritable((float)card_amt));
          context.write(key, new FloatWritable((float)(card_amt*100)/(float)total_tip));
          // % of card type paying tips 
          context.write(key, new FloatWritable((float)(100*count)/(float)card_cnt));
          context.write(key, new FloatWritable((float)count));
          context.write(key, new FloatWritable((float)card_cnt));
        }
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "tips3");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(Tips3.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
