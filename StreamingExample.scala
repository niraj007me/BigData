  import org.apache.spark._
  import org.apache.spark.SparkContext._
  import org.apache.log4j._
  import java.lang._
  import scala.io.Source
  import java.nio.charset.CodingErrorAction
  import scala.io.Codec

  object BroadCastExample {

   def parseLine() : Map[Int, String] = {
    var movieNames:Map[Int, String] = Map()
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val lines = Source.fromFile("/home/cloudera/Downloads/spark-data/movie-description").getLines()

    for (line <- lines) 
    {
     if (line.length>0) {
       val fields = line.split('|')
       movieNames= movieNames+ (fields(0).toInt -> fields(1).toString)
     }}

     return movieNames
   }

   def main (args :Array[String])
   {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc= new SparkContext("local[*]","BraodCastExample")
    var nameDict = sc.broadcast(parseLine)
    val movie_data =sc.textFile("/home/cloudera/Downloads/spark-data/movie-data.data")

    val movie_data_ln = movie_data.map(x => (x.split("\t")(1).toInt, 1))
    val movieCounts = movie_data_ln.reduceByKey( (x, y) => x + y )
    val flipped = movieCounts.map( x => (x._2, x._1))
    val sortedMovies = flipped.sortByKey()

    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2 ),x._1))
    val results = sortedMoviesWithNames


    results.foreach(println)
    scala.io.StdIn.readLine()


  }

  }