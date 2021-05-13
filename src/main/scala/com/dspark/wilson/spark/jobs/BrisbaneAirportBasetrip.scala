package com.dspark.wilson.spark.jobs
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit,when}


object BrisbaneAirportBasetrip {


  def main(args: Array[String]) {

    // check for args availability before getting paths
    val (sd, ed,sa1_code,airportname) =
      args.size match {
        case x if x == 4 => (args(0), args(1),args(2),args(3))   // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYY-MM-DD(start date)  (2) YYYY-MM-DD(end date) (3) airport sa1 code (4) Airport Name")
          sys.exit(1)
        }
      }

    // create spark session
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    //date period
    val daterange = daySequence(sd,ed)


    for (date<- daterange) {


      //read in geo table
      val geohierarchy = spark.read.parquet("s3://au-daas-compute/output/parquet/aggregated-geo-hierarchy/" + date)
      val origin_geo = geohierarchy.withColumnRenamed("sa1", "origin_sa1")
        .withColumnRenamed("gcc", "origin_gcc")
        .withColumnRenamed("state", "origin_state")
        .select("geo_hierarchy_base_id", "origin_sa1","origin_gcc","origin_state")
      val dest_geo = geohierarchy.withColumnRenamed("sa1", "dest_sa1")
        .withColumnRenamed("gcc", "dest_gcc")
        .withColumnRenamed("state", "dest_state")
        .select("geo_hierarchy_base_id", "dest_sa1","dest_gcc","dest_state")

      val home_geo = geohierarchy.withColumnRenamed("sa1", "home_sa1")
        .withColumnRenamed("sa2", "home_sa2")
        .withColumnRenamed("sa3", "home_sa3")
        .withColumnRenamed("gcc", "home_gcc")
        .withColumnRenamed("state", "home_state")
        .withColumnRenamed("geo_hierarchy_base_id", "home_geo_unit_id")
        .select("home_geo_unit_id", "home_sa1","home_sa2","home_sa3", "home_gcc", "home_state")

      val work_geo = geohierarchy.withColumnRenamed("sa1", "work_sa1")
        .withColumnRenamed("sa2", "work_sa2")
        .withColumnRenamed("sa3", "work_sa3")
        .withColumnRenamed("gcc", "work_gcc")
        .withColumnRenamed("state", "work_state")
        .withColumnRenamed("geo_hierarchy_base_id", "work_geo_unit_id")
        .select("work_geo_unit_id", "work_sa1","work_sa2","work_sa3", "work_gcc", "work_state")

      val trip = {
        val path = if (date < "20191201") {("s3://au-daas-latest/output/parquet/trip/" + date + "/*/").mkString}
        else {("s3a://au-daas-compute/output/parquet/trip/" + date).mkString}
        spark.read.parquet(path).repartition(320)
      }
      // read in weight
      val weight = {
        val path = if (date <= "20191130") {("s3://au-daas-latest/xtrapolation_for_roamer/merge_imsi_weight/" + date).mkString}
        else {("s3://au-daas-compute/xtrapolation_for_roamer/merge_imsi_weight/" + date).mkString}

        spark.read.format("csv").option("header", "false").load(path).toDF("agentId", "weight")
      }

      //read in agent_profile
      val agent_profile_path =
        date match {
          case x if (x >= "20190101" && x<="20190131") || (x >= "20190301" && x<="20190331") || (x >= "20191001" && x<="20191031")=> "s3://au-daas-compute/output-v3/parquet-v3/agent-profile/"
          case x if x >= "20191201" && x<="20191231" => "s3://au-daas-compute/output/parquet/agent-profile/"
          case _ => "s3://au-daas-compute/output-v2/parquet-v2/agent-profile/"
        }

      val agent_profile = spark.read.parquet(agent_profile_path + date).withColumn("mark", lit(1))

      //join
      val trip_join = trip.join(weight, Seq("agentId"))
        .join(agent_profile, Seq("agentId"), "left")
        .withColumn("islocal", when($"mark".isNull, 0).otherwise(1))
        .join(origin_geo, trip.col("startGeoUnitId") === origin_geo.col("geo_hierarchy_base_id")).drop("geo_hierarchy_base_id")
        .join(dest_geo, trip.col("endGeoUnitId") === dest_geo.col("geo_hierarchy_base_id")).drop("geo_hierarchy_base_id")
        .filter($"origin_sa1" === sa1_code || $"dest_sa1" === sa1_code)


      trip_join.join(home_geo, Seq("home_geo_unit_id"), "left") //use left join to keep all record in trip_join
        .join(work_geo, Seq("work_geo_unit_id"), "left")
        .select($"agentId",$"weight",$"startGeoUnitId",$"endGeoUnitId", $"startTime", $"endTime", $"dominantMode", $"purpose", $"distance", $"duration", $"trajectory",
          $"origin_sa1",$"origin_gcc",$"origin_state", $"dest_sa1",$"dest_gcc",$"dest_state",$"home_sa1",$"home_sa2",$"home_sa3", $"home_gcc", $"home_state",
          $"work_sa1",$"work_sa2",$"work_sa3", $"work_gcc", $"work_state",
          $"linksInfo", $"islocal", $"originStaypointType", $"destStaypointType", $"originBuilding", $"destBuilding",
          $"listOfModes",$"OriginalListOfLinks",$"stopIDs",$"parentIDs",$"listOfLinks")
        .withColumnRenamed("agentId","agent_id")
        .repartition(2, $"agent_id")
        .sortWithinPartitions("agent_id")
        .write
        .mode("overwrite")
        .parquet(("s3://au-daas-users/wilson/clients/tmr_airport/" + airportname + "/tripStartOrEndAirport/" + date).mkString)
    }
    //stop spark
    spark.stop
  }

  //Day seq udf
  def daySequence(startDate: String, endDate: String, dash: Boolean = false): List[String] = {
    val start = LocalDate.parse(startDate)
    val end = LocalDate.parse(endDate)

    if (dash) {
      (0 to DAYS.between(start, end).toInt)
        .map(i => start.plusDays(i).toString)
        .toList
    } else {
      (0 to DAYS.between(start, end).toInt)
        .map(i => start.plusDays(i).toString.replaceAll("\\-", ""))
        .toList
    }
  }

}
