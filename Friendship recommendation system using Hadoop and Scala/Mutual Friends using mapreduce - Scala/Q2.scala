/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.util.Properties

object Q2 {
	def main(args: Array[String]) {


		val conf = new SparkConf().setAppName("Question-2").setMaster("local")

		val sc = new SparkContext(conf)
		val lines = sc.textFile("soc-LiveJournal1Adj.txt")

		//get first user
		val ID1 = readLine("1st User ID : ")

		//get second user
		val ID2 = readLine("2nd User ID : ")

		//get friends list for user 1
		val ID1_friends = lines.map(line=>line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line=>(ID1==line(0))).flatMap(line=>line(1).split(","))

		//get friends list of user 2
		val ID2_friends = lines.map(line=>line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line=>(ID2==line(0))).flatMap(line=>line(1).split(","))

		//merge them to get common friends
		val mutual_friends = ID1_friends.intersection(ID2_friends).collect()
		
		val result=ID1+","+ID2+"\t"+mutual_friends.mkString(",")
		//print the result on console
//		fin.foreach { x => println(x)}
		println(result)

	}
}
