package sparktoys.setsim

import org.apache.spark.{SparkConf, SparkContext}

object SetSimJoin {

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val threshold = args(2).toDouble
    val conf = new SparkConf().setAppName("SetsSimJoin")
    val sc = new SparkContext(conf)

    val frequency_dic = sc.textFile(inputFile).flatMap(_.split(" ").drop(0)).map(x=>(x,1)).reduceByKey(_+_).collectAsMap()
    val dic = sc.broadcast(frequency_dic)
    // (id, content_sorted)
    val inputSorted = sc.textFile(inputFile).map(x=>x.split(" ")).map(x=>(x(0).toInt,x.tail.sortBy(x=>(dic.value(x),x)))).map(x => {
      val freqList = x._2
      val elementstring = freqList.mkString(" ")
      (x._1.toInt, elementstring)
    }
    ).sortByKey(true)

    val input = inputSorted.map(x => s"${x._1} ${x._2}")

    val result = input.map(line => (line.split(" "))).flatMap(
      n =>{
        for( k:Int <- 1 to ((n.length - 1 - Math.ceil(threshold * (n.length - 1))).toInt + 1))yield{
          (n(k), (n(0), n.takeRight(n.length-1).mkString(" ")))
        }
      }).groupByKey().flatMap(f => {
      for(i <- 0 to f._2.toArray.length - 1)yield{
        for(j <- i+1 to f._2.toArray.length - 1)yield{
          val firstList = f._2.toArray.apply(i)._2.split(" ")
          val firstListId = f._2.toArray.apply(i)._1

          val secondList = f._2.toArray.apply(j)._2.split(" ")
          val secondListId = f._2.toArray.apply(j)._1

          val intersectLength = firstList.intersect(secondList).length.toDouble
          val unionLength =  firstList.length + secondList.length - intersectLength

          val jarccardDistance = intersectLength/unionLength

          val keyFirst = Math.min(firstListId.toInt, secondListId.toInt)
          val keySecond = Math.max(firstListId.toInt, secondListId.toInt)
          ((keyFirst, keySecond), jarccardDistance)
        }
      }
    }).flatMap(x => x).filter(f => f._2 >= threshold).distinct().map(_.swap).sortByKey(false).map(_.swap)
      .sortBy(s => (s._1._1, s._1._2), true).collect().map(f => s"${f._1}\t${f._2}")

    val outputRDD = sc.parallelize(result)
    outputRDD.saveAsTextFile(outputFile)
  }
}

