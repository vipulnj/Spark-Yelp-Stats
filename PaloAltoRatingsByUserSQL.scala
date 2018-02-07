import org.apache.spark._
import org.apache.spark.sql.SparkSession

object PaloAltoRatingsByUserSQL{
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Common Friends Count Detailed SQL").setMaster("local")
		val sc = new SparkContext(conf)

		val sparkSession = SparkSession.builder().getOrCreate()
		import sparkSession.implicits._

		val businessDF = sparkSession.read.format("csv").option("delimiter", ":").load("business.csv").selectExpr("_c0 as business_id", "_c2 as address").distinct()
		val paloAltoBusinessesDF = businessDF.filter($"address".contains("Palo Alto")).selectExpr("business_id")

		val reviewDF = sparkSession.read.format("csv").option("delimiter", ":").load("review.csv").selectExpr("_c2 as user_id", "_c4 as bus_id", "_c6 as stars")
		reviewDF.show()

		val resDF = paloAltoBusinessesDF.join(reviewDF, paloAltoBusinessesDF("business_id") === reviewDF("bus_id")).select("user_id", "stars")
		//println(resDF.count())
		resDF.write.option("delimiter","\t").csv("output")
	}
}