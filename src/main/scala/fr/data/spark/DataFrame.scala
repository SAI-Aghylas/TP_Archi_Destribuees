package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fr.data.spark.ReaderCsvFile
object DataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("code_postal_tp")
      .master("local[2]")
      .getOrCreate()
    val reader= ReaderCsvFile(spark)
    val df_code_insee= reader.readcsv("src/main/resources/code-insee-postaux-geoflar.csv")
    val df_code_insee_columns=reader.storeColumn(df_code_insee)
    //affichage du nbr de communes:
    //val c1=df.select("Code_commune_INSEE").distinct().count()
    //print("nombre de commmunes:"+c1)
    //afficher le nombre de communes qui ont l'attribut ligne_5

  }


}
