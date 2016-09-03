import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object naive_bayes {

	def main(args: Array[String]) {

		System.setProperty("hadoop.home.dir", "E:\\winutil\\")
		val conf = new SparkConf().setAppName("naive_bayes").setMaster("local")

		val sc = new SparkContext(conf)
		val data = sc.textFile("data/glass.data")
		val parsedData = data.map { line =>
		val parts = line.split(',')
		LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
		}
		// Split data into training (60%) and test (40%).
		val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
				val training = splits(0)
				val test = splits(1)

				val model = NaiveBayes.train(training, lambda = 1.0)

				val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
				val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
				println("Accuracy: "+accuracy)
	}
}