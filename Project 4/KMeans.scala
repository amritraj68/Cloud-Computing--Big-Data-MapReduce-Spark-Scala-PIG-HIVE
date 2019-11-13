import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import math.hypot


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()

  /* Function to calculate the distance*/
  def distance(a: Point, b: Point): Double ={

   var distance = math.hypot(a._1 - b._1, a._2 - b._2)
   distance
}

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    /* Reading the centroids.txt file */
    centroids = sc.textFile(args(1)).map(
     line =>
      { 
      	val cols = line.split(",")
        (cols(0).toDouble,cols(1).toDouble)
      }
      ).collect

    /* Reading the points-small.txt or points-large.txt file */
    var points=sc.textFile(args(0)).map(
      line=>
    	{
    		val k=line.split(",")
           (k(0).toDouble,k(1).toDouble)
      }
      )
     
    /* Calculating the new centroids*/ 
    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map {
         case(c,range)=>
          var count=0
          var sumX=0.0
          var sumY=0.0
 
          for(w <- range) {
            count += 1
            sumX+=w._1
            sumY+=w._2
        } 
        var NewCentroidX=sumX/count
        var NewCentroidY=sumY/count
        (NewCentroidX,NewCentroidY)

      }.collect
    }

centroids.foreach(println)
    }
}