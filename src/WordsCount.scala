import org.apache.spark.SparkContext

object WordsCount extends App{
  val sc = new SparkContext("local[*]", "wordscount")
  val textInput = sc.textFile("C:\\Users\\sandip\\Downloads\\search_data.txt")
  val words = textInput.flatMap(x => x.split(" "))
  val wordsMap = words.map(x=> (x,1))
  val wordsCount = wordsMap.reduceByKey((x,y) => (x+y))
  //hello
  wordsCount.collect.foreach(println)
}
