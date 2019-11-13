import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import scala.math.max

object Partition {

  val depth =6

def mapperFunction(i:(Long,Long,List[Long]))= {
   var lst = new ListBuffer[(Long,Long)]
   lst+=((i._1,i._2))
   if(i._2 > 0) {
    for(x<-i._3)
     lst+=((x,i._2)) }
   lst
  }

     def reduce_func(key:(Long,(Long,(Long,List[Long]))))={
      var cen = key._2._2._1
           if(key._2._2._1 == (-1).toLong){
             cen = key._2._1
           }
           
             (key._1,cen,key._2._2._2)
           
         } // end of reduce_func function

  def main(args: Array[String]): Unit = {

    val conf =new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var count=0

    /* Reading the Graph from the text file ..The graph cluster id is set to -1 except for the first 5 nodes*/
    var graph = sc.textFile(args(0)).map(x=>x.split(",")).map(x=>x.map(_.toLong)).map(
      x=>
      if(count < 5){
        count+=1;
        (x(0),x(0),x.drop(1).toList) }
        else {
          count+=1 ;(x(0),-1.toLong,x.drop(1).toList)} 
          ) // end of map ..end of reading the graph


/* Processing the Graph */

    for(i<- 1 to depth)
    {
      graph = graph.flatMap{mapperFunction(_)}.reduceByKey(max).join(graph.map(x=>{(x._1,(x._2,x._3))})).map{reduce_func(_)}
    }
    graph.map(x=>(x._2,1)).reduceByKey(_+_).collect().foreach(println)
  } // end of main



       }