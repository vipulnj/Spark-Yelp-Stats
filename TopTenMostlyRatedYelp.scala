import org.apache.spark._

object TopTenMostlyRatedYelp{

	def parseReviewCSV(line: String) = {
		val cols = line.split("::")

		val review_id = cols(0)
		val business_id = cols(2)

		(business_id, review_id)
	}

	def parseBusinessCSV(line: String) = {
		val cols = line.split("::")

		val business_id = cols(0)
		val addr = cols(1)
		val categories = cols(2)

		(business_id, (addr, categories))
	}

//	def getBusinessDetailsById(business_id:String, details: Iterable[(String, String)]) = {
//		val det = details.toList
//		val  addrnCategories = det(0)
//		println(addrnCategories)
//		(business_id, addrnCategories)
//	}

	// t: (numberOfRatings, (address, categoriesList))
	def getBusinessDetailsById(business_id: String, details:(Int, (String, String))) = {
		val addr = details._2._1
		val categories = details._2._2
		val ratedBy = details._1

		(business_id, addr, categories, ratedBy)
	}

	def main(args: Array[String]): Unit = {

		val conf = new SparkConf().setAppName("RatingsCount").setMaster("local")
		val sc = new SparkContext(conf)

		val reviewRDD = sc.textFile("review.csv").map(parseReviewCSV)
		val businessReviewersRDD = reviewRDD.groupByKey()

		val topTenBusinessReviewedCount = businessReviewersRDD.map(x => (x._1, x._2.size)).sortBy(x => x._2 * -1).coalesce(1, false).take(10)
		val topTenBusinessReviewedCountRDD = sc.parallelize(topTenBusinessReviewedCount)
		//topTenBusinessReviewedCountRDD.foreach(println)

		val businessRDD = sc.textFile("business.csv").map(parseBusinessCSV).groupByKey().map(x => (x._1, x._2.head))
		val resultRDD = topTenBusinessReviewedCountRDD.join(businessRDD).map(x =>(x._1, x._2._2._1, x._2._2._2 ,x._2._1)).sortBy(x => x._4 * -1)
	    	.map(x => x._1 + "\t" + x._2 + "\t" + x._3 + "\t" + x._4)

		resultRDD.saveAsTextFile("output")

	}
}
