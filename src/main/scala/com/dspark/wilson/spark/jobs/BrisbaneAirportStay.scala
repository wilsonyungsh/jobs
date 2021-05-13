package com.dspark.wilson.spark.jobs

import com.dspark.wilson.spark.jobs.BrisbaneAirportBasetrip.daySequence
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit,when,to_timestamp}

object BrisbaneAirportStay {

  def main(args: Array[String]) {

    // check for args availability before getting paths
    val (sd, ed, sa1_code, airportname) =
      args.size match {
        case x if x == 4 => (args(0), args(1), args(2), args(3)) // check input parameters
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
    val daterange = daySequence(sd, ed)
    val geohierarchy = spark.read.parquet("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/latest")
    val geo = geohierarchy.selectExpr("sa1", "sa2", "sa3", "state", "geo_hierarchy_base_id as geo_unit_id")
    val home_geo = geohierarchy.selectExpr("sa1 as home_sa1", "sa2 as home_sa2", "sa3 as home_sa3", "state as home_state", "geo_hierarchy_base_id as home_geo_unit_id")
    val work_geo = geohierarchy.selectExpr("sa1 as work_sa1", "sa2 as work_sa2", "sa3 as work_sa3", "state as work_state", "geo_hierarchy_base_id as work_geo_unit_id")

    //specify airport sa1 to filter to
    val airport_sa1 = geo.filter('sa1 === sa1_code)

    for (date <- daterange) {
      // read in staypoint
      val stay = {
        val path = if (date < "20191201") {
          ("s3a://au-daas-latest/output/parquet/union_staypoint_enriched/" + date + "/*/").mkString
        }
        else {
          ("s3a://au-daas-compute/output/parquet/union_staypoint_enriched/" + date).mkString
        }
        println("staypoint path : " + path)
        spark.read.parquet(path).repartition(20)
      }
      // read in weight
      val weight = {
        val path = if (date <= "20190430") {
          ("s3a://au-daas-latest/xtrapolation_for_roamer/merge_imsi_weight/" + date).mkString
        }
        else if (date <= "20191130") {
          ("s3a://au-daas-compute-sg/xtrapolation_for_roamer-v2/merge_imsi_weight/" + date).mkString
        }
        else {
          ("s3a://au-daas-compute/xtrapolation_for_roamer/merge_imsi_weight/" + date).mkString
        }
        println("weight path : " + path)
        spark.read.format("csv").option("header", "false").load(path).toDF("agentId", "weight").withColumnRenamed("agentId", "agent_id")
      }

      //read in agent_profile
      val agent_profile = {
        val path = if (date <= "20191130") {
          ("s3a://au-daas-compute/output-v2/parquet-v2/agent-profile/" + date).mkString
        }
        else {
          ("s3a://au-daas-compute/output/parquet/agent-profile/" + date).mkString
        }
        println("agent_profile path : " + path)
        spark.read.parquet(path).withColumn("mark", lit(1))
      }
      //join agent with weight and geoh
      val agent_wt_joined = weight.join(agent_profile, Seq("agent_id"), "left")
        .join(home_geo, Seq("home_geo_unit_id"), "left")
        .join(work_geo, Seq("work_geo_unit_id"), "left")
        .withColumn("islocal", when($"mark".isNull, 0).otherwise(1))
        .drop("mark")
      //join sp and calculate duration
      val sp_step1 = stay.join(airport_sa1, Seq("geo_unit_id"), "inner") //filtering for stay in airport sa1 only
        .join(agent_wt_joined, Seq("agent_id"), "inner")
        .withColumn("durationInSec", to_timestamp('out_time).cast("Long") - to_timestamp('in_time).cast("Long"))

      // output
      sp_step1.write.mode("overwrite")
        .parquet(("s3a://au-daas-users/wilson/clients/tmr_airport/" + airportname + "/stayInAirportSA1/" + date).mkString)
    }
  }
}
