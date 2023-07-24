import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object WordsCount extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordscount")
  val textInput = sc.textFile("C:\\Users\\sandip\\Downloads\\search_data.txt")
  val words = textInput.flatMap(x => x.split(" "))
  val wordsLower = words.map(x=>x.toLowerCase())
  val wordsMap = wordsLower.map(x=> (x,1))
  val wordsCount = wordsMap.reduceByKey((x,y) => (x+y))
  wordsCount.collect.foreach(println)

  //THIS WILL KEEP THE TERMINAL LIVE, SO WE WILL BE ABLE TO SEE THE DAG
  // scala.io.StdIn.readLine()

  //We can using chaining to resolve the above problem
  /*sc.textFile("C:\\Users\\sandip\\Downloads\\search_data.txt").
    flatMap((_.split(" "))).
    map(_.toLowerCase()).
    map((_,1)).
    reduceByKey(_ + _).
    collect.
    foreach(println)*/
}
