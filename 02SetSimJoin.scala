package sparktoy.setsim
 
import org.apache.spark.{SparkConf, SparkContext}
 
object SetSimJoin {
 
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("SetsSimJoin").setMaster("local")
    val sc = new SparkContext(conf)
 
    val frequency_dic = sc.textFile(inputFile).flatMap(_.split(" ").drop(0)).map(x=>(x,1)).reduceByKey(_+_).collectAsMap()
    val dic = sc.broadcast(frequency_dic)
    // (id, content_sorted)
    val inputSorted = sc.textFile(inputFile).map(x=>x.split(" ")).map(x=>(x(0).toInt,x.tail.sortBy(x=>(dic.value(x),x)))).map(x => {
      val freqList = x._2.toList
      val elementstring = freqList.mkString(" ")
      (x._1.toInt, elementstring)
    }
    ).sortByKey(true)
 
    val input = inputSorted.map(x => s"${x._1} ${x._2}")
 
    val middleResult = input.map(x => (x.split(" "))).flatMap(
        n =>{
         val elementListLength = n.length - 1
         for( k <- 1 to ((elementListLength - Math.ceil(threshold * elementListLength)).toInt + 1))yield{
           (n(k).toInt, (n(0), n.takeRight(n.length-1)))
         }
        }).distinct()
     
    val Result = middleResult.groupByKey().map(x => (x._1, x._2.toList.distinct)).flatMap(x => x._2.toList.combinations(2)).distinct()
      .map(x => ((x(0)._1,x(1)._1), 
          x(0)._2.intersect(x(1)._2).length.toDouble / 
          (x(0)._2.length.toDouble + x(1)._2.length.toDouble 
          - x(0)._2.intersect(x(1)._2).length.toDouble) )).distinct()
          .filter(f => f._2 >= threshold).distinct().map(_.swap).distinct()
          .map(x =>(x._1.toDouble, (Math.min(x._2._1.toDouble, x._2._2.toDouble).toInt, Math.max(x._2._1.toDouble, x._2._2.toDouble).toInt)))
          .distinct()
          .sortByKey(false).map(_.swap).sortBy(s => (s._1._1.toInt, s._1._2.toInt), true).map(f => s"${f._1}\t${f._2}").saveAsTextFile(outputFile)
  }
}