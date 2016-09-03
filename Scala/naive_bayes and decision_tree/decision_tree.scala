import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini

object decision_tree {

	def main(args: Array[String]) {

		System.setProperty("hadoop.home.dir", "E:\\winutil\\")
		val conf = new SparkConf().setAppName("decision_tree").setMaster("local")

		val sc = new SparkContext(conf)
		val data = sc.textFile("data/glass.data")
		val parsedData = data.map { line =>
		val parts = line.split(',')
		LabeledPoint(parts(10).toDouble, Vectors.dense(parts(0).toDouble,parts(1).toDouble,parts(2).toDouble,parts(3).toDouble,parts(4).toDouble,parts(5).toDouble,parts(6).toDouble,parts(7).toDouble,parts(8).toDouble,parts(9).toDouble))
		}

		val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
				val training = splits(0)
				val test = splits(1)


				val numClasses = 8
				val categoricalFeaturesInfo = Map[Int, Int]()
				val impurity = "gini"
				val maxDepth = 5
				val maxBins = 100

				val model = DecisionTree.trainClassifier(parsedData, numClasses, categoricalFeaturesInfo,
						impurity, maxDepth, maxBins)

						val labelAndPreds = test.map { point =>
						val prediction = model.predict(point.features)
						(point.label, prediction)
		}
		val accuracy =  1.0 *labelAndPreds.filter(r => r._1 == r._2).count.toDouble / test.count
				println("Accuracy = " + accuracy)
	}
}