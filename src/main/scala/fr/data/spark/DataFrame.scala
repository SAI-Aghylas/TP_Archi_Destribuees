package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import fr.data.spark.ReaderCsvFile
object DataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("code_postal_tp")
      .master("local[2]")
      .getOrCreate()
    //reading files:
    val reader= ReaderCsvFile(spark)
    val df_code_insee= reader.readcsv("src/main/resources/code-insee-postaux-geoflar.csv")
    val df_code_insee_columns=reader.storeColumn(df_code_insee)
    //df_code_insee.show()
    val df_communes = reader.readcsv("src/main/resources/Communes.csv")
    val df_communes_columns=reader.storeColumn(df_communes)
    //df_communes.show()
    val df_postes = reader.readcsv("src/main/resources/postesSynop.txt")
    val df_postes_columns=reader.storeColumn(df_postes)
    //df_postes.show()
    val df_meteo = reader.readcsv("src/main/resources/synop.2020120512.txt")
    val df_meteo_columns=reader.storeColumn(df_communes)
    //df_meteo.show()


    //begin selection
    val code_insee=df_code_insee.select("CODE INSEE","Code Dept","geom_x_y")
    val geom_x_y = df_code_insee.select("geom_x_y")
    geom_x_y.show()
    //code_insee.show()
    val commune=df_communes.select("DEPCOM","PTOT")
    //commune.show()
    val postes= df_postes.select("ID","Latitude","Longitude")
    //postes.show()
    val udfTemperature=udf((s:Double)=>(s-273.15).toInt)
    val meteo= df_meteo.withColumn("t",udfTemperature(df_meteo("t").alias("t_k")))
    //meteo.show()
    val meteo2 =meteo.select("t","date","numer_sta")


    //Jointures:
    val df_comune_code_insee = code_insee.join(commune,commune("DEPCOM")===code_insee("CODE INSEE"),"left").drop(code_insee("CODE INSEE"))
    df_comune_code_insee.show()

    val df_postes_meteo2= postes.join(meteo2,meteo2("numer_sta")===postes("ID"),"left").drop(postes("ID"))
    df_postes_meteo2.show()

    //writing files:
   // reader.writeParket(df_comune_code_insee,"src/main/resources/comune_insee")
   // reader.writeParket(df_postes_meteo2,"src/main/resources/postes_meteo")


  }


}
