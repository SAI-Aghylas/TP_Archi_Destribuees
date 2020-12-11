package fr.data.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
class ReaderCsvFile(spark: SparkSession){
  def readcsv(path:String): DataFrame ={
    spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(path)
  }
  def storeColumn(df: DataFrame):List[String] ={
    df.columns.toList
  }
  def  distance (p1_lat: Double, p1_lon: Double, p2_lat: Double, p2_lon: Double) :Double= {
    scala.math.sqrt(scala.math.pow(p2_lat-p1_lat,2) + scala.math.pow(p2_lon - p1_lon,2))
  }
  def writeParket(dataFrame: DataFrame,p:String)={
    dataFrame.write.parquet(path = p)
  }
}
object ReaderCsvFile{
  def apply(spark: SparkSession): ReaderCsvFile = new ReaderCsvFile(spark)
}
