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
}
object ReaderCsvFile{
  def apply(spark: SparkSession): ReaderCsvFile = new ReaderCsvFile(spark)
}
