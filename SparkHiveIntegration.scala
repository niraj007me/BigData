import java.io.File
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
/*

Error:The root scratch dir: /tmp/hive on HDFS should be writable. Current permissions are: rwx----
Resolution: Set hive-site.xml and grant usning  "sudo chmod -R 777 /tmp/hive"
Edit vi /etc/hive/conf/hive-site.xml, Press Insert +modify+esx+:q+:x
<property>
    <name>hive.exec.scratchdir</name>
    <value>/tmp/hive</value>
    <description>Scratch space for Hive jobs</description>
  </property>
  <property>
    <name>hive.scratch.dir.permission</name>
    <value>777</value>
    <description>The permission for the user-specific scratch directories that get created in the root scratch directory </description>
  </property>
  

*/

object SparkHive {
  def main (args :Array [String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
  .builder()
  .appName("Spark Hive Example")
  .master("local[*]")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .enableHiveSupport()
  .getOrCreate()
  
   import spark.sql
   
    case class Record(key: Int, value: String)


// warehouseLocation points to the default location for managed databases and tables

sql("CREATE TABLE IF NOT EXISTS TEST_HIVE_table (key INT, value STRING) USING hive")
sql("LOAD DATA LOCAL INPATH '/home/cloudera/Documents/dataset/kv.txt' INTO TABLE TEST_HIVE_table")

// Queries are expressed in HiveQL
sql("SELECT * FROM TEST_HIVE_table").show()
  }
  
}