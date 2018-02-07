import org.apache.spark._

object PaloAltoRatingsByUser{

	def parsePaloAltoBusinesses(line: String) = {
		val rec = line.split("::")
		val business_id = rec(0)
		val addr = rec(1)
		if (addr.contains("Palo Alto"))
			(business_id, addr) // addr not actually needed
		else
			(business_id, "None")
	}

	def parseReviews(line: String) = {
		val rec = line.split("::")
		val user_id = rec(1)
		val business_id = rec(2)
		val stars = rec(3)
		(business_id, (user_id, stars))
	}

	def main(args: Array[String]) = {

		val conf = new SparkConf().setAppName("PaloAltoRatingsByUser").setMaster("local")
		val sc = new SparkContext(conf)

		val businessCSV = sc.textFile("business.csv")
		val paloAltoBusinessesRDD = businessCSV.map(parsePaloAltoBusinesses).filter(x => {x._2 != "None"}).groupByKey().map(x => (x._1, x._2.head))
		//paloAltoBusinessesRDD.foreach(println)

		val reviewRDD = sc.textFile("review.csv").map(parseReviews)

		val joinedRDD = paloAltoBusinessesRDD.join(reviewRDD)
		//joinedRDD.foreach(println)

		val resRDD = joinedRDD.map(x => (x._2._2._1, x._2._2._2))
		resRDD.saveAsTextFile("output")

	}
}
