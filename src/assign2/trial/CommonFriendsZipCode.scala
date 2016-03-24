import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable.ListMap
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Set

object CommonFriendZipCode {
	val INPUT_DIR = "/Users/pavanyr/Documents/Code/Java/Bigdata_DataSet/friend_list.txt"
	val USER_INPUT_DIR = "/Users/pavanyr/Documents/Code/Java/Bigdata_DataSet/userdata.txt"

	val IS_FRIEND = -1
	val IS_NOT_FRIEND = 1

	def emitCommonFriends(args: String): Array[String] = {
			var emit = new ArrayBuffer[(String, (String, Int))]()
			val userAndFriend = args.split("\\t")
			userAndFriend(1).split(",")
  }
	
	def emitZipCode(args: String): Array[(String, (String, Int))] = {
					var emit = new ArrayBuffer[(String, (String, Int))]()
							val userAndFriend = args.split("\\t")
							// Check for line validity
							if (userAndFriend.length < 2) {
								return emit.toArray            // should not return null
							}

					val id = userAndFriend(0)
							val friendList = userAndFriend(1).split(",")
							// emitting direct friends
							for(i <- 0 until friendList.length) {
								emit += ((id, (friendList(i), IS_FRIEND)))
							}
					// emiting indirect friends
					for(i <- 0 until friendList.length) {
						for(j <- i+1 until friendList.length) {
							emit += ((friendList(i), (friendList(j), IS_NOT_FRIEND)))
							emit += ((friendList(j), (friendList(i), IS_NOT_FRIEND)))
						}
					}
					emit.toArray
	}

	def reduceProcess(args:(String, Iterable[(String, Int)])): String = {
			val key = args._1
					var friendMap = Map[String, Int]()
					args._2.foreach(friend => {
						val friendId = friend._1
								val relation = friend._2
								if (friendMap.contains(friendId)) {
									if (friendMap(friendId) == IS_FRIEND) {
										// Ignore. He is already direct friend
									}
									else if (relation.equals(IS_FRIEND)) { // id, friendId are direct friends
										friendMap += (friendId -> IS_FRIEND)
									} else { // (id, friendId) have one another mutual friend
										friendMap += (friendId -> (friendMap(friendId) + 1))
									}
								} else {
									if (relation.equals(IS_FRIEND)) {
										friendMap += (friendId -> IS_FRIEND)
									} else {
										friendMap += (friendId -> 1)
									}
								}
					})
					// sort the candidates by the count of mutual friends with target user
					val friendMapSorted = ListMap(friendMap.toSeq.sortWith(_._2 > _._2):_*)
					var count = 0
					val recommendedFriends = new StringBuilder()
			    recommendedFriends.append(key+"\t")
					val loop = new Breaks
					loop.breakable {
			      for((friendId, relation) <- friendMapSorted) {
			        if(relation == IS_FRIEND) loop.break()
    			    count += 1
    			    if(count != 1) recommendedFriends.append(", ")
    			    recommendedFriends.append(friendId)
				    }
					}
			recommendedFriends.toString()
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Friend Recommendation Project").setMaster("local")
		conf.set("spark.hadoop.validateOutputSpecs", "false")
		val sc = new SparkContext(conf)
		val input = sc.textFile(INPUT_DIR)
		val userDetails = sc.textFile(USER_INPUT_DIR)
		val targetID1 = "0"
		val targetID2 = "1"
		val id1Friends = input.map( line => {
		  val l = line.split("\t")
		  if(l.length == 2)
		    (l(0), l(1))
		   else
		     ("-1","0")
		}).
		filter { id => (id._1.equals(targetID1))}.
		flatMap { friendList => friendList._2.split(",")}
		val id2Friends = input.map( line => {
		  val l = line.split("\t")
		  if(l.length == 2)
		    (l(0), l(1))
		   else
		     ("-1","0")
		}).
		filter { id => (id._1.equals(targetID2))}.
		flatMap { friendList => friendList._2.split(",")}
		var commonFriends = id1Friends.intersection(id2Friends).collect()
		val mapZipCodes = userDetails.map(line => {
		 val l = line.split(",")
		 (l(0), l(6))
		}).filter(f => commonFriends.contains(f._1)).map(l => {(l._1, l._2)})
		println(mapZipCodes.count())
		print(targetID1 +", " + targetID2 + ": ")
		mapZipCodes.foreach({
		  print
		})
	}
}

