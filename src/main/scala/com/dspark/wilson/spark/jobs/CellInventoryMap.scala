package com.dspark.wilson.spark.jobs

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.sql.functions.{concat, expr, input_file_name, lit, regexp_replace} //

object CellInventoryMap {

  def main(args: Array[String]) {
    //set up parameters
    val (date) =
      args.size match {
        case x if x == 1 => (args(0)) // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMMDD for day of cell inventory for processing")
          sys.exit(1)
        }
      }

    // setup sedona instance
    val spark = SparkSession.builder()
      //.appName("sedona") // Change this to a proper name
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .config("geospark.global.index", "true") //index
      .config("geospark.global.indextype", "quadtree")
      .config("geospark.join.gridtype", "quadtree")
      .config("geospark.join.numpartition", 50)
      .getOrCreate()

    //register Sedona SQL
    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    val inputPath = "s3://au-daas-compute/input/cell-inventory/" + date + "/*"
    val fields = "site_id,site_name,lac,lga_name,site_type,gda_94_longitude,gda_94_latitude,technology,band,cell_sector_no,cell_id,cell_name,sac_id,sa3_code,rnc_bsc,cell_first_seen,antenna_azimuth,beam_width_horizontal,beam_width_vertical,antenna_electric_down_tilt,antenna_mechanical_down_tilt,antenna_height,antenna_gain,antenna_type,cell_power,feeder_length,feeder_size,cell_status"
    val cellInventory = spark.read.format("csv").option("delimiter", "|").load(inputPath).toDF(fields.split(","): _*)
      .withColumn("file_date", regexp_replace(input_file_name(), "^.*?(\\d{8}).*$", "$1"))
      .withColumn("cgi", concat(lit("505-02-"), 'lac, lit("-"), 'cell_id))
      .select("cgi", "site_name", "cell_name", "cell_status", "lga_name", "sa3_code", "site_type", "gda_94_longitude", "gda_94_latitude", "file_date")
      .withColumn("gda_94_longitude", 'gda_94_longitude.cast("Double"))
      .withColumn("gda_94_latitude", 'gda_94_latitude.cast("Double"))
      .withColumn("geom", expr("st_point(CAST(gda_94_longitude AS Decimal(24,20)),CAST(gda_94_latitude AS Decimal(24,20)))"))

    //load sa1 and ssc geometry
    val sa1_path = "s3://au-daas-users/wilson/boundary/sa1/"
    val sa1 = spark.read.format("csv").option("delimiter", "|").option("header", "true").load(sa1_path)
      .selectExpr("SA1_MAIN16 as sa1", "SA2_MAIN16 as sa2", "SA2_NAME16 as sa2_name", "STE_CODE16 as state", "STE_NAME16 as state_name", "ST_GeomFromWKT(geometry) as geom")

    val ssc_path = "s3://au-daas-users/wilson/boundary/ssc/"
    val ssc = spark.read.format("csv").option("delimiter", "|").option("header", "true").load(ssc_path)
      .selectExpr("SSC_CODE16 as suburb_code", "SSC_NAME16 as suburb_name", "ST_GeomFromWKT(geometry) as geom")

    //register to sql table
    cellInventory.createOrReplaceTempView("cell")
    sa1.repartition(200).createOrReplaceTempView("sa1_geom")
    ssc.repartition(200).createOrReplaceTempView("ssc_geom")

    //spatial join
    val cell_complete = spark.sql(
      "select z.*,y.suburb_name,y.suburb_code from (select a.*,b.sa1,b.sa2,b.sa2_name,b.state,b.state_name from cell a,sa1_geom b where st_within(a.geom,b.geom)) z,ssc_geom y where st_within(z.geom,y.geom)"
    ).drop("geom")

    val output_path = "s3://au-daas-users/wilson/cell_inventory/" + date
    cell_complete.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).format("csv").save(output_path)


    //stop spark
    spark.stop
  }
}
