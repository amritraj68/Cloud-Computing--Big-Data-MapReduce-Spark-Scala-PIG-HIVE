import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Point implements WritableComparable<Point> {
    
    public double x;
    public double y;
   
    
    Point(){
        super();
       // System.out.println("Default Constructor");
    }
    
    Point(double x, double y){
        this.x = x;
        this.y=y;
    }
    @Override
    public void write(DataOutput d) throws IOException {
        
        d.writeDouble(x);
        d.writeDouble(y);
       
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        
        x = di.readDouble();
        y=di.readDouble();
    
    }

    @Override
    public int compareTo(Point point) {
        
        double xValue = this.x;
        double xPointValue = point.x;
        double yValue=this.y;
        double yPointValue=point.y;

        if(xValue < xPointValue)
         { return -1;}
        else if(xValue>xPointValue){
            return 1;
        }
        else{
            if(yValue < yPointValue)
         { return -1;}
        else if(yValue>yPointValue){
            return 1;
        }
        }
        
        return 0;
       
    }
    
    @Override
     public String toString() {
                return this.x + "," + this.y;
        }

}

public class KMeans {
    
    static Vector<Point> centroids = new Vector<Point>(100);
    
    public static class AvgMapper extends Mapper<Object,Text,Point,Point> {

        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException 
        {   
           // super.setup(context);
            
            URI[] paths = context.getCacheFiles();
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(paths[0]))));
            
            String line ;
            line = reader.readLine();
            while(line != null){   
                
               StringTokenizer str = new StringTokenizer(line,",");
              // System.out.println(str);
                while(str.hasMoreElements())
                {
                 
                    Double x = Double.parseDouble(str.nextElement().toString());
                    Double y = Double.parseDouble(str.nextElement().toString());
                    
                    Point p = new Point(x,y);
                    centroids.add(p);
                    line = reader.readLine();
            }
               // System.out.println("Centroids"+centroids);
                
            }
        }        
        
        @Override
        protected void map(Object key, Text values, Context context) throws IOException, InterruptedException {
         //   super.map(key, value, context); //To change body of generated methods, choose Tools | Templates.
            
            Scanner scanner = new Scanner(values.toString()).useDelimiter(",");
            Point p = new Point (scanner.nextDouble(),scanner.nextDouble());
            double distance;
            double minimum = Double.MAX_VALUE;
            Point closestCentroid = new Point();
            
            for (Point point : centroids){
                
                distance = Math.sqrt((p.x-point.x)*(p.x-point.x) + ((p.y-point.y)*(p.y-point.y)));
                if(distance<minimum){
                    minimum = distance ;
                    closestCentroid = point ;
                } // end of if loop
            } // end of for loop
           // System.out.println("closestCentroid x,y" + closestCentroid.x +","+closestCentroid.y);
            //System.out.println("Point x,y" +"p.x"+","+"p.y");
            context.write(closestCentroid,p);
        }    
    }

    public static class AvgReducer extends Reducer<Point,Point,Point,Object> {

        @Override
        protected void reduce(Point key, Iterable<Point> values, Context context) throws IOException, InterruptedException
        {
            
            int count = 0;
            double sx = 0.0;
            double sy = 0.0 ;
            
            for (Point p : values){
                count++;
                sx += p.x;
                sy += p.y ;
            }
                key.x = sx/count ;
                key.y = sy/count;
                context.write(key,null);
        } // end of reduce method
        
        
        
    } // End of AvgReducer method

    public static void main ( String[] args ) throws Exception 
    {
        System.out.println("*****In the Driver Code*****");
        
	//Configuration Details w.r.to JOB,JAR etc
    
        Configuration conf = new Configuration();
   
        Job job = new Job(conf, "KMEANS JOB");
    
        job.setJarByClass(KMeans.class);
   
        job.setMapperClass(AvgMapper.class);
    
        job.setMapOutputKeyClass(Point.class);
   
        job.setMapOutputValueClass(Point.class);
    
        job.setReducerClass(AvgReducer.class);
  
        job.setOutputKeyClass(Point.class); 
   
        job.setOutputValueClass(Object.class);  
        
        job.addCacheFile(new URI(args[1]));
   
        job.setInputFormatClass(TextInputFormat.class);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
     
        job.setOutputFormatClass(TextOutputFormat.class);
   
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
   
        //System Exit process
    
        job.waitForCompletion(true);
    }
}

