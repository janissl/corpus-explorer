package com.tilde.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.reflect.io.File


object CorpusExplorer {

  def getMaxWordLength: UserDefinedFunction = udf(
    (wordList: Seq[String]) => (for (word <- wordList) yield word.length).max
  )

  def getLongWords: UserDefinedFunction = udf(
    (wordList: Seq[String]) => for (word <- wordList if word.length > 20) yield word
  )

  def removeOldOutput(file: File): Unit = {
    if (file.exists) {
      if (file.isFile) {
        file.delete
      } else if (file.isDirectory) {
        file.deleteRecursively
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    val appName = CorpusExplorer.getClass.getSimpleName.replaceAll("""\$$""", "")
    //    val spark = SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    val spark = SparkSession.builder.appName(appName).getOrCreate()
    import spark.implicits._

    val df = spark
      .read
      .option("encoding", "UTF-8")
      .option("quote", "")
      .option("header", "false")
      .textFile(args(0))

    println("Unique sentences: %d (from %d)".format(df.dropDuplicates.count, df.count))

    val wordsDF = df
      .withColumn("words", split(df("value"), """\s+"""))
      .withColumnRenamed("value", "sentence")


    val sentLengthDF = wordsDF
      .withColumn("sentence_length", size($"words"))
      .drop($"words")

    val sentencesByLengthFile = File(args(0) + ".sentences_by_length")
    removeOldOutput(sentencesByLengthFile)

    sentLengthDF
      .sort($"sentence_length".desc, $"sentence")
      .coalesce(1)
      .write
      .csv(sentencesByLengthFile.path)

    val sentLengthDistributionFile = File(args(0) + ".sentence_length_distribution")
    removeOldOutput(sentLengthDistributionFile)

    sentLengthDF
      .drop("sentence")
      .groupBy($"sentence_length")
      .count
      .sort($"sentence_length".desc)
      .coalesce(1)
      .write
      .csv(sentLengthDistributionFile.path)

    val wordCountDF = df
      .flatMap(_.split("""\s+"""))
      .map(_.replaceAll("""^\W+|[^\w']+$""", ""))
      .map((_, 1))
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      .groupBy($"word")
      .count

    val wordCountFile = File(args(0) + ".word_count")
    removeOldOutput(wordCountFile)

    wordCountDF
      .sort($"word")
      .coalesce(1)
      .write
      .csv(wordCountFile.path)

    println("Unique words: %d (from %d)".format(
      wordCountDF.count,
      sentLengthDF
        .select(sum($"sentence_length"))
        .first
        .getLong(0)))

    val wordLengthDF = wordCountDF
      .withColumn("word_length", length($"word"))
      .drop($"count")

    val wordCountsByLengthFile = File(args(0) + ".word_length_distribution")
    removeOldOutput(wordCountsByLengthFile)

    wordLengthDF
      .groupBy($"word_length")
      .count
      .sort($"word_length".desc)
      .coalesce(1)
      .write
      .csv(wordCountsByLengthFile.path)

    val wordsByLengthFile = File(args(0) + ".words_by_length")
    removeOldOutput(wordsByLengthFile)

    wordLengthDF
      .sort($"word_length".desc, $"word")
      .coalesce(1)
      .write
      .csv(wordsByLengthFile.path)

    val charCountDF = df
      .flatMap(_.split(""))
      .map((_, 1))
      .withColumnRenamed("_1", "char")
      .withColumnRenamed("_2", "count")
      .groupBy($"char")
      .count

    val charCountFile = File(args(0) + ".char_count")
    removeOldOutput(charCountFile)

    charCountDF
      .sort($"char")
      .coalesce(1)
      .write
      .csv(charCountFile.path)

    val charsByFrequencyFile = File(args(0) + ".char_distribution")
    removeOldOutput(charsByFrequencyFile)

    charCountDF
      .sort($"count".desc)
      .coalesce(1)
      .write
      .csv(charsByFrequencyFile.path)

    println("The whole processing completed in %.5f seconds".format((System.currentTimeMillis() - startTime) / 1000f))
  }
}
