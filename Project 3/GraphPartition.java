import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

 class Vertex implements Writable {

        public long id;                   // the vertex ID
        public Vector<Long> adjacent;     // the vertex neighbors
        public long centroid;             // the id of the centroid in which this vertex belongs to
        public short depth;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public Vector<Long> getAdjacent() {
            return adjacent;
        }

        public void setAdjacent(Vector<Long> adjacent) {
            this.adjacent = adjacent;
        }

        public long getCentroid() {
            return centroid;
        }

        public void setCentroid(long centroid) {
            this.centroid = centroid;
        }

        public short getDepth() {
            return depth;
        }

        public void setDepth(short depth) {
            this.depth = depth;
        }

        public Vertex() {
        }

        public Vertex(long id, Vector adjacent, long centroid, short depth) {
            this.id = id;
            this.centroid = centroid;
            this.adjacent = adjacent;
            this.depth = depth;
        }

        public final void readFields(DataInput di) throws IOException {

            this.id = di.readLong();

            this.centroid = di.readLong();
            this.depth = di.readShort();
            this.adjacent = readVector(di);
        }

        public static Vector readVector(DataInput in) throws IOException {
            Vector<Long> vect = new Vector<Long>();
            Long u;
            int i = 1;
            while (i > 0) {
                try {
                    if ((u = in.readLong()) != -1) {
                        vect.add(u);
                    } else {
                        i = 0;
                    }
                } catch (EOFException eof) {
                    i = 0;
                }

            }
            return vect;
        }

        public void write(DataOutput d) throws IOException {
            d.writeLong(this.id);
            d.writeLong(this.centroid);
            d.writeShort(this.depth);
            for (Long adjacent1 : this.adjacent) {
                d.writeLong(adjacent1);
            }
        }

        public String toString() {
            return (id + "," + adjacent + "," + centroid + "," + depth);
        }
    }

public class GraphPartition {

    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    public static short BFS_depth = 0;

    public static class FirstMapper extends Mapper<LongWritable, Text, LongWritable, Vertex> {

        
        private int count=0;
        private Short depth=0;

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] nodes = line.split(",");
            long id = Long.parseLong(nodes[0]);
            long centroid;
            Vector<Long> adjacent = new Vector<Long>();
            for (int i = 1; i < nodes.length; i++) {
                adjacent.add(Long.parseLong(nodes[i]));
            }
            if (count < 10) {
                centroid = id;
                con.write(new LongWritable(id), new Vertex(id, adjacent, centroid, depth));
            } else {
                centroid = -1;
                con.write(new LongWritable(id), new Vertex(id, adjacent, -1, depth));
            }
            count += 1;
        } // end of map method
    } // end of FirstMapper

    public static class SecondMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        public void map(LongWritable key, Vertex value, Context context)
                throws IOException, InterruptedException {
            
            context.write(new LongWritable(value.getId()), value);
            
            Long centroid = value.getCentroid();
            Vector<Long> v = new Vector<Long>();
            
            v = value.getAdjacent();
            if (centroid > 0) {
                for (Long v1 : v) {
                    context.write(new LongWritable(v1), new Vertex(v1, centroids, centroid, BFS_depth));
                } // end of for loop
            } // end of if loop
        } // end of map method
    } // end of SecondMapper

    public static class SecondReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException
        {
            Short min_depth = 1000;
            Vertex m = new Vertex(-1, centroids, -1, (short)0);
            for (Vertex v : values) {
                if (!v.getAdjacent().isEmpty()) {
                    m.setAdjacent(v.getAdjacent());
                }
                if (v.getCentroid() > 0 && v.getDepth() < min_depth) {
                    min_depth = v.getDepth();
                    m.setCentroid(v.getCentroid());
                }
                m.setId(v.getId());
            } // end of for loop
            m.setDepth(min_depth);
            context.write(key, m);
        } // end of reduce method
    } // end of SecondReducer

    public static class ThirdMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {

        public void map(LongWritable key, Vertex values, Context context)
                throws IOException, InterruptedException {
            
            context.write(new LongWritable(values.getCentroid()), new IntWritable(1));
        }
    }

    public static class ThirdReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int m = 0;
            for (IntWritable v : values) {
                m = m + v.get();
            }
            context.write(key, new IntWritable(m));
        }
    }

    // Main entry point for the program
    public static void main(String[] args) throws Exception {
        
        System.out.println("WITHIN THE DRIVER CLASS");
        /*if (args.length != 3) 
		{
			System.err.println("Usage: MRChainingUsecase: <HDFSInput path> <HDFS-INTERMEDIATE-PATH1> <HDFS-PATH2");
			System.exit(-1);
		}*/
        
        /* ... First Map-Reduce job to read the graph */

        System.out.println("**** STARTING OF FIRST JOB ****");
        Job job1 = Job.getInstance();
        job1.setJobName("First Job");
        /* ... First Map-Reduce job to read the graph */
        job1.setJarByClass(GraphPartition.class);
        
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        
        job1.setMapperClass(FirstMapper.class);
        
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/i0"));
        
        //Setting Input File Format - INPUT--> "TextInputFormat" , 
	// OUTPUT--> "SequenceFileInputFormat" for job1 that will act as an intermediate output and used as input for job2
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.waitForCompletion(true);

        System.out.println("Start of Second job");
        
        for (short i = 0; i < max_depth; i++) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job2.setJobName("Secong Job");
            job2.setJarByClass(GraphPartition.class);
            
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            
            job2.setMapperClass(SecondMapper.class);
            job2.setReducerClass(SecondReducer.class);
            
            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/i" + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/i" + (i + 1)));
            
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            job2.waitForCompletion(true);
        }

        System.out.println("Start of Third job");
        Job job3 = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job3.setJobName("GraphPartition");
        job3.setJarByClass(GraphPartition.class);
        
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(IntWritable.class);
        
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        
        job3.setMapperClass(ThirdMapper.class);
        job3.setReducerClass(ThirdReducer.class);
        
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/i8"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.waitForCompletion(true);
    } // end of main() method
} // end of Driver Class
