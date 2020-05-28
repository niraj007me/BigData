import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import java.util.Properties
import org.apache.spark.SparkContext._



object DataFrameJson {
  def main (args :Array[String]){
  
  val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
      
  import spark.implicits._
      Logger.getLogger("org").setLevel(Level.ERROR)

  
  val df = spark.read.json("/home/cloudera/Documents/File-Format-data/JsonFIle.json")
  // df.show()
  // df.select("name").show()
   df.select($"name", ($"pages_i" + 1).as("New PageID")).show()
   df.filter($"pages_i" >385).show()
   df.groupBy("id").count().show()
   df.createTempView("tempTable")
   val sqlDF = spark.sql("SELECT * FROM tempTable where pages_i between 64 and 384")
   sqlDF.show()

  
  }
  
}