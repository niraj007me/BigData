import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.lang._

object BroadCastExample {
  
   def parseLine(line: String) = {
      // Split by commas
      val fields = line.split('|')
      // Extract the age and numFriends fields, and convert to integers
      val id = fields(0).toInt
      val name = fields(1).toString
      // Create a tuple that is our result.
      (id, name)
  } 
  
  
  def main (args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc= new SparkContext("local[1]","BraodCastExample")
    val movie_name = sc.textFile("/home/cloudera/Downloads/spark-data/movie-description")
    
    val row =movie_name.map(x=>parseLine(x)).collect()
    var nameDict = sc.broadcast(row)
    //  val results1=nameDict.value
     
    //results1.foreach(println) 
        
   
    val movie_data =sc.textFile("/home/cloudera/Downloads/spark-data/movie-data.data")
   // val movie_data_ln= movie_data.map(x=> (x.split("\t")(1),1))
   val movie_data_ln = movie_data.map(x => (x.split("\t")(1).toInt, 1))
       val movieCounts = movie_data_ln.reduceByKey( (x, y) => x + y )
    val flipped = movieCounts.map( x => (x._2, x._1) )
    val sortedMovies = flipped.sortByKey()
    // sortedMovies.foreach(x=>println(x))
   // println(sortedMovies)
    
 
   val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2 ),x._1))
   val results = sortedMoviesWithNames.collect()
     
   
  results.foreach(x=>println(x))
    
    
  }
  
}