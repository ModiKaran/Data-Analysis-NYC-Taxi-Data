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

import java.awt.Point;  
import java.awt.geom.Point2D;  
import java.util.Arrays;  

public class FinalHM {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {


    public class Polygon2D {  
   
	    private int npoints;  
	   
	    private double xpoints[];  
	   
	    private double ypoints[];  
	   
	    private static final int MIN_LENGTH = 4;  
	   
	    /** 
	     * Creates an empty polygon. 
	     */  
	    public Polygon2D() {  
		xpoints = new double[MIN_LENGTH];  
		ypoints = new double[MIN_LENGTH];  
	    }  
	   
	    /** 
	     * Constructs and initializes a <code>Polygon</code> from the specified 
	     * parameters. 
	     * @param xpoints an array of X coordinates 
	     * @param ypoints an array of Y coordinates 
	     * @param npoints the total number of points in the 
	     *                          <code>Polygon</code> 
	     * @exception  NegativeArraySizeException if the value of 
	     *                       <code>npoints</code> is negative. 
	     * @exception  IndexOutOfBoundsException if <code>npoints</code> is 
	     *             greater than the length of <code>xpoints</code> 
	     *             or the length of <code>ypoints</code>. 
	     * @exception  NullPointerException if <code>xpoints</code> or 
	     *             <code>ypoints</code> is <code>null</code>. 
	     */  
	    public Polygon2D(double xpoints[], double ypoints[], int npoints) {  
		if (npoints > xpoints.length || npoints > ypoints.length) {  
		    throw new IndexOutOfBoundsException("npoints > xpoints.length || "+  
		                                        "npoints > ypoints.length");  
		}  
		if (npoints < 0) {  
		    throw new NegativeArraySizeException("npoints < 0");  
		}  
		this.npoints = npoints;  
		this.xpoints = Arrays.copyOf(xpoints, npoints);  
		this.ypoints = Arrays.copyOf(ypoints, npoints);  
	    }  
	   
	    /** 
	     * Resets this <code>Polygon</code> object to an empty polygon. 
	     * The coordinate arrays and the data in them are left untouched 
	     * but the number of points is reset to zero to mark the old 
	     * vertex data as invalid and to start accumulating new vertex 
	     * data at the beginning. 
	     * All internally-cached data relating to the old vertices 
	     * are discarded. 
	     * Note that since the coordinate arrays from before the reset 
	     * are reused, creating a new empty <code>Polygon</code> might 
	     * be more memory efficient than resetting the current one if 
	     * the number of vertices in the new polygon data is significantly 
	     * smaller than the number of vertices in the data from before the 
	     * reset. 
	     */  
	    public void reset() {  
		npoints = 0;  
	    }  
	   
	    /** 
	     * Appends the specified coordinates to this <code>Polygon</code>. 
	     * <p> 
	     * If an operation that calculates the bounding box of this 
	     * <code>Polygon</code> has already been performed, such as 
	     * <code>getBounds</code> or <code>contains</code>, then this 
	     * method updates the bounding box. 
	     * @param       x the specified X coordinate 
	     * @param       y the specified Y coordinate 
	     */  
	    public void addPoint(double x, double y) {  
		if (npoints >= xpoints.length || npoints >= ypoints.length) {  
		    int newLength = npoints * 2;  
		    // Make sure that newLength will be greater than MIN_LENGTH and  
		    // aligned to the power of 2  
		    if (newLength < MIN_LENGTH) {  
		        newLength = MIN_LENGTH;  
		    } else if ((newLength & (newLength - 1)) != 0) {  
		        newLength = Integer.highestOneBit(newLength);  
		    }  
	   
		    xpoints = Arrays.copyOf(xpoints, newLength);  
		    ypoints = Arrays.copyOf(ypoints, newLength);  
		}  
		xpoints[npoints] = x;  
		ypoints[npoints] = y;  
		npoints++;  
	    }  
	   
	    /** 
	     * Determines whether the specified {@link Point} is inside this 
	     * <code>Polygon</code>. 
	     * @param p the specified <code>Point</code> to be tested 
	     * @return <code>true</code> if the <code>Polygon</code> contains the 
	     *                  <code>Point</code>; <code>false</code> otherwise. 
	     * @see #contains(double, double) 
	     */  
	    public boolean contains(Point p) {  
		return contains(p.x, p.y);  
	    }  
	   
	    /** 
	     * Determines whether the specified coordinates are inside this 
	     * <code>Polygon</code>. 
	     * <p> 
	     * @param x the specified X coordinate to be tested 
	     * @param y the specified Y coordinate to be tested 
	     * @return {@code true} if this {@code Polygon} contains 
	     *         the specified coordinates {@code (x,y)}; 
	     *         {@code false} otherwise. 
	     * @see #contains(double, double) 
	     */  
	    public boolean contains(int x, int y) {  
		return contains((double) x, (double) y);  
	    }  
	   
	    public boolean contains(double x, double y) {  
		if (npoints <= 2) {  
		    return false;  
		}  
		int hits = 0;  
	   
		double lastx = xpoints[npoints - 1];  
		double lasty = ypoints[npoints - 1];  
		double curx, cury;  
	   
		// Walk the edges of the polygon  
		for (int i = 0; i < npoints; lastx = curx, lasty = cury, i++) {  
		    curx = xpoints[i];  
		    cury = ypoints[i];  
	   
		    if (cury == lasty) {  
		        continue;  
		    }  
	   
		    double leftx;  
		    if (curx < lastx) {  
		        if (x >= lastx) {  
		            continue;  
		        }  
		        leftx = curx;  
		    } else {  
		        if (x >= curx) {  
		            continue;  
		        }  
		        leftx = lastx;  
		    }  
	   
		    double test1, test2;  
		    if (cury < lasty) {  
		        if (y < cury || y >= lasty) {  
		            continue;  
		        }  
		        if (x < leftx) {  
		            hits++;  
		            continue;  
		        }  
		        test1 = x - curx;  
		        test2 = y - cury;  
		    } else {  
		        if (y < lasty || y >= cury) {  
		            continue;  
		        }  
		        if (x < leftx) {  
		            hits++;  
		            continue;  
		        }  
		        test1 = x - lastx;  
		        test2 = y - lasty;  
		    }  
	   
		    if (test1 < (test2 / (lasty - cury) * (lastx - curx))) {  
		        hits++;  
		    }  
		}  
	   
		return ((hits & 1) != 0);  
	    }  
	   
	    public boolean contains(Point2D p) {  
		return contains(p.getX(), p.getY());  
	    }  
    }


    private Text word = new Text();
    private static Polygon2D[] p = new Polygon2D[310];

    @Override
    public void setup(Context context) {
        try
    	{
		File text = new File("op.txt");
		Scanner scanner = new Scanner(text);
		int k = 0;
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			String strArray[] = line.split(" ");
			double[] x = new double[strArray.length];
			double[] y = new double[strArray.length];
			for(int i=0; i < strArray.length; i++){
		               	String arr2[] = strArray[i].split(",");
				x[i] = Double.parseDouble(arr2[0]);
				y[i] = Double.parseDouble(arr2[1]);
		       	}
			Polygon2D ob = new Polygon2D(x,y,strArray.length);
			p[k] = ob;
			k += 1;
		}
    	}
    	catch(Exception e)
    	{
		System.out.println(e);
    	}
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	String[] foo = value.toString().split(",");
	try{
		double cop1 = Double.parseDouble(foo[11]);
		double cop2 = Double.parseDouble(foo[10]);
		double cod1 = Double.parseDouble(foo[13]);
		double cod2 = Double.parseDouble(foo[12]);
		
		for(int i=0;i<310;i++)
		{
			if(p[i].contains(cop1,cop2))
			{
				context.write(new Text(""+i), new IntWritable(1));
			}
			if(p[i].contains(cod1,cod2))
			{
				context.write(new Text(""+i), new IntWritable(1));
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
        
        Job job = new Job(conf, "finalhm");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(FinalHM.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

