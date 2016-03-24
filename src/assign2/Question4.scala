package assign2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author sappy
 */
object Q4 {
    def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "E:\\winutil\\")
    val conf = new SparkConf().setAppName("Question-4").setMaster("local")

        val sc = new SparkContext(conf)
    val lines = sc.textFile("/home/sappy/files/soc-LiveJournal1Adj.txt")
    val friends = lines.map(line=>line.split("\\t")).filter(l1 => (l1.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>Array((z,x._1))))
    val userData = sc.textFile("/home/sappy/files/userdata.txt")
    val aa = userData.map(line=>line.split(",")).map(line=>(line(0),2016-line(9).split("/")(2).toFloat))
    val a=friends.join(aa)
    def mean1(xs: Iterable[Float]) = xs.sum / xs.size
    val avgRatings = a.groupBy(_._2._1).mapValues(xs=>mean1(xs.map(_._2._2))).toArray
    val sortedRatings = avgRatings.sortBy(_._2).reverse
    val top20 = sortedRatings.take(20)
    val top20sc = sc.parallelize(top20)
    val aaa = userData.map(line=>line.split(",")).map(line=> (line(0),line(1)+","+line(3)+","+line(4)+","+line(5)))
    val byKey = aaa.map({case(id,address) => id->address})
    val ajoin = byKey.join(top20sc).sortBy(_._2,false)
    val req = ajoin.map(x=> x._2)
    
    req.foreach(x=> println(x.toString().replace("(", " ").replace(")", " ")))
  
  }
}
