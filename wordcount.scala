---------------------Sample1---------
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object wordcount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    l,k("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","wordcount")   
    
    val input = sc.textFile("/home/cloudera/Downloads/spark-data/book-data.txt")
   
    val words = input.flatMap(a => a.split(" "))
    
    val wordCounts = words.map(x => (x,1))
    
    val final_count = wordCounts.reduceByKey((a,b) => a+b)
    
    val my_count = final_count.collect()
    
    final_count.map(x => x._1 + "," + x._2).saveAsTextFile("/home/cloudera/Downloads/spark-data/output")

    // final_count.saveAsTextFile("/home/cloudera/Downloads/spark-data/output")
    my_count.foreach(println)
    
    scala.io.StdIn.readLine()
  }
  
}

-------------------Sample 2---------------------------------
// File path and Output location pass as argument
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.lang
import org.apache.spark.sql.Row

/** Count up how many of each word appears in a book as simply as possible. */
object wordcount {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","wordcount")   
    
    //val input1 = args(0).toString()
    
   
    // read in text file and split each document into words
      val words = sc.textFile(args(0)).flatMap(_.split(" "))


    
    val wordCounts = words.map(x => (x,1))
    
    val final_count = wordCounts.reduceByKey((a,b) => a+b)
    
    val my_count = final_count.collect()
    
    final_count.map(x => x._1 + "," + x._2).saveAsTextFile(args(1))

    //final_count.map(x => x._1 + "," + x._2).saveAsTextFile("/home/cloudera/Downloads/spark-data/output")

    // final_count.saveAsTextFile("/home/cloudera/Downloads/spark-data/output")
    my_count.foreach(println)
    
    scala.io.StdIn.readLine()
  }
  
}

