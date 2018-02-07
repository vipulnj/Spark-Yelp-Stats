import org.apache.spark._
import org.apache.spark.sql.SparkSession


object TopTenMostlyRatedYelpSQL{
	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("Common Friends Count Detailed SQL").setMaster("local")
		val sc = new SparkContext(conf)

		val sparkSession = SparkSession.builder().getOrCreate()
		import sparkSession.implicits._

		val reviewDF = sparkSession.read.format("csv").option("delimiter", ":").load("review.csv").selectExpr("_c0 as review_id", "_c4 as business_id")

		val topTenBusinessReviewsDF = reviewDF.groupBy('business_id).count().orderBy($"count".desc).limit(10)

		val businessDF = sparkSession.read.format("csv").option("delimiter", ":").option("inferSchema", "true").load("business.csv").selectExpr("_c0 as bus_id", "_c2 as full_address", "_c4 as categories").distinct()

		val resDF = topTenBusinessReviewsDF.join(businessDF, topTenBusinessReviewsDF("business_id") === businessDF("bus_id"))
			.selectExpr("business_id", "full_address", "categories", "count")
		//resDF.show()

		resDF.write.option("delimiter","\t").csv("output")

	}
}
