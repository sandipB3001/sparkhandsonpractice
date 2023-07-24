import org.apache.spark.SparkContext

object WordsCount extends App{
  val sc = new SparkContext("local[*]", "wordscount")
  
}
