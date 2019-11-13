import java.io.*;
import java.net.URI;
import java.util.*;
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

} // end of Point Class

class Avg implements Writable
{
    public double sumX;
    public double sumY;
    public long count;
    
    Avg()
    {
        super();
        // Default Constructor
    }
    
    Avg(double sumX,double sumY, long count){
        
        this.sumX = sumX;
        this.sumY = sumY;
        this.count = count;
        
    }

    @Override
    public void write(DataOutput d) throws IOException {
        
        d.writeDouble(sumX);
        d.writeDouble(sumY);
        d.writeLong(count);
       
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        
        sumX = di.readDouble();
        sumY = di.readDouble();
        count = di.readLong();
    
    }
} // end of class Avg

public class KMeans {
    
    static Vector<Point> centroids = new Vector<Point>(100);
    
    static Hashtable<Point, Avg> table ;
    
    public static class AvgMapper extends Mapper<Object,Text,Point,Avg> {

        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException 
        {   
            super.setup(context);
            table = new Hashtable<Point,Avg>();
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
                } // end of StringTokenizer While loop
               // System.out.println("Centroids"+centroids);
            } // end of reading line While loop 
        }    // end of setup method    

        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {
            Set<Point> keys = table.keySet();
            for(Point c : keys){
                context.write(c, table.get(c));
            }
             
        } // end of cleanup method
        
        @Override
        protected void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            
            Scanner scanner = new Scanner(values.toString()).useDelimiter(",");
            Point p = new Point (scanner.nextDouble(),scanner.nextDouble());
            double distance;
            double minimum = Double.MAX_VALUE;
            int count = 0;
            
            Point closestCentroid = new Point();
            
            for (Point point : centroids)
            {
                count++;
                distance = Math.sqrt((p.x-point.x)*(p.x-point.x) + ((p.y-point.y)*(p.y-point.y)));
                if(distance<minimum){
                    minimum = distance ;
                    closestCentroid = point ;
                } // end of if loop
                
            }// end of for loop
            
            if(table.get(closestCentroid)== null)
            {
                table.put(closestCentroid,new Avg(p.x,p.y,1));
            }
            else
            {
                table.put(closestCentroid, new Avg(table.get(closestCentroid).sumX+p.x,table.get(closestCentroid).sumY+p.y,table.get(closestCentroid).count+1));
            }
            
        }   // end of Map Method 
    } // end of Mapper Class

    public static class AvgReducer extends Reducer<Point,Avg,Point,Object> {

        @Override
        protected void reduce(Point key, Iterable<Avg> values, Context context) throws IOException, InterruptedException
        {
            
            long count = 0;
            double sx = 0.0;
            double sy = 0.0 ;
            
            for (Avg a : values){
                count += a.count;
                sx += a.sumX;
                sy += a.sumY ;
                
            }
                key.x = sx/count ;
                key.y = sy/count;
                context.write(key,null);
        } // end of reduce method
        
    } // End of AvgReducer method

    // Main Entry point for the program
    public static void main ( String[] args ) throws Exception 
    {
        System.out.println("*****In the Driver Code*****");
        
	//Configuration Details w.r.to JOB,JAR etc
    
        Configuration conf = new Configuration();
   
        Job job = new Job(conf, "KMEANS JOB");
    
        job.setJarByClass(KMeans.class);
   
        job.setMapperClass(AvgMapper.class);
    
        job.setMapOutputKeyClass(Point.class);
   
        job.setMapOutputValueClass(Avg.class);
    
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

