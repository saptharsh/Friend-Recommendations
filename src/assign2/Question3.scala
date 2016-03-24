package assign2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author sappy
 */
object Question3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("QuestionThree").setMaster("local")
    val sc = new SparkContext(conf)

    // 8715, 8932
    val user1 = readLine("Enter First User : ")
    val user2 = readLine("Enter Second User : ")
    val lines = sc.textFile("/home/sappy/files/soc-LiveJournal1Adj.txt")
    val friends1 = lines.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (user2 == line(0))).flatMap(line => line(1).split(","))
    val friends2 = lines.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (user1 == line(0))).flatMap(line => line(1).split(","))

    val mutuals = friends2.intersection(friends1).collect()

    val userData = sc.textFile("/home/sappy/files/userdata.txt")
    val details = userData.map(line => line.split(",")).filter(line => mutuals.contains(line(0))).map(line => (line(1), line(6)))

    // Checking for data
    /*
    val count = details.count()
    println(count)
    */
    
    println(user1+" "+ user2 +" "+ "["+details.map(a=>a._1+":"+a._2).collect.mkString(",")+"]")

  }
}
