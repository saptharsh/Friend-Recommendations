package assign2

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author sappy
 */
object Question1 {

  def sort_friends(friends: List[(Int, Int)]) : List[Int]   = {

      friends.sortBy(tup_pair => (-tup_pair._2, tup_pair._1)).map(tup_pair => tup_pair._1)

  }
  def main(args: Array[String]) {

    val OUTPUT_DIREC = "/home/sappy/files/output/"
    val conf = new SparkConf().setAppName("Question-1").setMaster("local")

    val sc = new SparkContext(conf)
    val logData = sc.textFile("/home/sappy/files/soc-LiveJournal1Adj.txt")

    val pairs_of_friend = logData.map(line=>line.split("\\t")).filter(line => (line.size == 2)).map(line=>(line(0),line(1).split(","))).flatMap(x=>x._2.flatMap(z=>Array((x._1.toInt,z.toInt))))

    val SelfJoin = pairs_of_friend.join(pairs_of_friend)

    val allFriends = SelfJoin.map(elem => elem._2).filter(elem => elem._1 != elem._2)

    val MutualFriends = allFriends.subtract(pairs_of_friend)

    val pair_friends = MutualFriends.map(pair_with_a_mutual_friend => (pair_with_a_mutual_friend, 1))

    val recommended_friend = pair_friends.reduceByKey((a, b) => a + b).map(elem => (elem._1._1, (elem._1._2, elem._2))).groupByKey().

    map(tup2 => (tup2._1, sort_friends(tup2._2.toList))).map(tup2 => tup2._1.toString + "\t" + tup2._2.map(x=>x.toString).toArray.mkString(","))
    .saveAsTextFile(OUTPUT_DIREC + "output")
    //    recommended_friend.foreach(x=> println(x))
  }
}
