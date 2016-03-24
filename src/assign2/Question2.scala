package assign2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author sappy
 */
object Question2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("QuestionTwo").setMaster("local")
    val sc = new SparkContext(conf)

    // 8715, 8932
    val user1 = readLine("Enter First User : ")
    val user2 = readLine("Enter Second User : ")
    val lines = sc.textFile("/home/sappy/files/soc-LiveJournal1Adj.txt")
    val friends1 = lines.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (user2.equals(line(0)))).flatMap(line => line(1).split(","))
    val friends2 = lines.map(line => line.split("\\t")).filter(l1 => (l1.size == 2)).filter(line => (user1.equals(line(0)))).flatMap(line => line(1).split(","))
    
    val result = friends2.intersection(friends1).collect.mkString(",")
    
    println(user1+" "+user2+" "+"["+result+"]")
        
  }
}
