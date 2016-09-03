/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.util.Properties

object Q3 {


	def main(args: Array[String]) {
	  
	  //make reference to winutils.exe - required if running in windows
		System.setProperty("hadoop.home.dir", "E:\\winutil\\")
		
		//set config - local
		val conf = new SparkConf().setAppName("Question-3").setMaster("local")
		val sc = new SparkContext(conf)

		//read network data
		val lines = sc.textFile("soc-LiveJournal1Adj.txt")

		//read users data
		val userData = sc.textFile("userdata.txt")

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

		//get details of mutual friends from user data
		val details = userData.map(line=>line.split(",")).filter(line=>mutual_friends.contains(line(0))).map(line=>(line(1)+":"+line(6)))
		//		.saveAsTextFile("output/")
		//		print the details of mutual friends on console
		details.foreach { x => println(x)}

		//print result in required format
		val result=ID1+" "+ID2+"\t"+"["+details.collect.mkString(",")+"]"

				println(result)

	}
}
