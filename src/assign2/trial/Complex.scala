class Complex(real: Double, imaginary: Double) {
  def re() = real
  def im = imaginary
  var temp : Int = 0
  def t = temp
  def test = "hello"
}
/**
class Friend(details : String) {
  private var _id = ""
  private var _friendList = List[String]_
  
  def id= _id 
  def id_= (value:String):Unit = _id = value 
  
  def friendList = _friendList
  def friendList_= (value:List[String]):Unit = _friendList= value.map{x -> x}
  
  def setFriendsList() = {
    var s : Array[String] = details.split("\t")
    id = s(0)
    friendList = List.fromArray((s(1).split(",")))
  }
}
**/