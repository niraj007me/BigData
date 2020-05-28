import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object SchemaOnRDD {
  
  def main (args : Array[String]){
     Logger.getLogger("org").setLevel(Level.ERROR)

       val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
      
     
     import spark.implicits._
     val fileRDD = spark.sparkContext.textFile("/home/cloudera/Downloads/spark-data/friends-data.csv") 
     val schemaString = "id name age count"
     val fields= schemaString.split(" ").map(fieldname => StructField(fieldname,StringType,nullable=true))
     val schema = StructType(fields)
     val rowRDD = fileRDD.map(x=>x.split(",")).map(fld=> Row(fld(0),fld(1),fld(2),fld(3)))
     val peopleDF = spark.createDataFrame(rowRDD, schema)
   //  peopleDF.show()
     // Creates a temporary view using the DataFrame
peopleDF.createOrReplaceTempView("people")

// SQL can be run over a temporary view created using DataFrames
val results = spark.sql("SELECT id,name FROM people").show()

// The results of SQL queries are DataFrames and support all the normal RDD operations
// The columns of a row in the result can be accessed by field index or by field name
//results.map(attributes => "Name: " + attributes(0)).show()
   
     
    
  }
  
}