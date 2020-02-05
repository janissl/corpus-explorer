package com.tilde.spark

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object CorpusExplorer {
  def printUsage(): Unit = {
    println("USAGE: spark-submit --class com.tilde.spark.CorpusExplorer --master local[*] " +
      "corpus-explorer-1.0-SNAPSHOT.jar ${path_or_pattern/to/plaintext/corpus/file} ${output_directory}")
  }

  def getFullstopTokens: UserDefinedFunction = udf(
    (wordList: Seq[String]) => {
      val lastWord = wordList.last
      if (lastWord.matches("""^.*\p{L}\.$""")) {
        lastWord
      } else {
        null
      }
    })

  def getMaxWordLength: UserDefinedFunction = udf(
    (wordList: Seq[String]) => (for (word <- wordList) yield word.length).max
  )

  def getLongWords: UserDefinedFunction = udf(
    (wordList: Seq[String]) => for (word <- wordList if word.length > 20) yield word
  )

  def outputDirExists(path: Path): Boolean = {
    try {
      Files.createDirectories(path)
      true
    } catch {
      case e: java.io.IOException => println(e.getMessage)
        false
    }
  }

  def removeOldOutput(path: Path): Unit = {
    if (Files.exists(path)) {
      if (Files.isRegularFile(path)) {
        Files.deleteIfExists(path)
      } else if (Files.isDirectory(path)) {
        path.toFile.listFiles.foreach(f => Files.deleteIfExists(f.toPath))
      }
    }
  }

  def explore(sourcePathPattern: String, outputDir: String): Unit = {
    val outputDirPath = Paths.get(outputDir)

    if (!outputDirExists(outputDirPath)) {
      throw new Exception("Could not create the output directory '%s'".format(outputDir))
    }

    val startTime = System.currentTimeMillis()

    val appName = CorpusExplorer.getClass.getSimpleName.replaceAll("""\$$""", "")
    val appMode = if (ManagementFactory.getRuntimeMXBean.getInputArguments.toString.indexOf("-agentlib:jdwp") > 0) "debug" else "run"

    val spark = if (appMode == "debug") {
      SparkSession.builder.appName(appName).master("local[*]").getOrCreate()
    } else {
      SparkSession.builder.appName(appName).getOrCreate()
    }

    import spark.implicits._

    val df = spark
      .read
      .option("encoding", "UTF-8")
      .option("quote", "")
      .option("header", "false")
      .textFile(sourcePathPattern)

    println("Unique sentences: %d (from %d)".format(df.dropDuplicates.count, df.count))

    val wordsDF = df
      .withColumnRenamed("value", "sentence")
      .withColumn("words", split($"sentence", """\p{Z}+"""))

    val fullstopTokenFilePath = outputDirPath.resolve("fullstop_tokens")

    wordsDF
      .withColumn("fullstopTokens", getFullstopTokens($"words"))
      .filter($"fullstopTokens".isNotNull)
      .groupBy($"fullstopTokens")
      .count
      .sort($"count".desc)
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(fullstopTokenFilePath.toString)

    val sentLengthDF = wordsDF
      .withColumn("sentence_length", size($"words"))
      .drop($"words")

    val sentencesByLengthFilePath = outputDirPath.resolve("sentences_by_length")
    removeOldOutput(sentencesByLengthFilePath)

    sentLengthDF
      .sort($"sentence_length".desc, $"sentence")
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(sentencesByLengthFilePath.toString)

    val sentLengthDistributionFilePath = outputDirPath.resolve("sentence_length_distribution")
    removeOldOutput(sentLengthDistributionFilePath)

    sentLengthDF
      .drop("sentence")
      .groupBy($"sentence_length")
      .count
      .sort($"sentence_length".desc)
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(sentLengthDistributionFilePath.toString)

    val wordCountDF = df
      .flatMap(_.split("""\p{Z}+"""))
      .map(_.replaceAll("""^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$""", ""))
      .filter(_.nonEmpty)
      .map((_, 1))
      .withColumnRenamed("_1", "word")
      .withColumnRenamed("_2", "count")
      .groupBy($"word")
      .count

    val wordCountFilePath = outputDirPath.resolve("word_count")
    removeOldOutput(wordCountFilePath)

    wordCountDF
      .sort($"word")
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(wordCountFilePath.toString)

    println("Unique words: %d (from %d)".format(
      wordCountDF.count,
      sentLengthDF
        .select(sum($"sentence_length"))
        .first
        .getLong(0)))

    val wordLengthDF = wordCountDF
      .withColumn("word_length", length($"word"))
      .drop($"count")

    val wordCountsByLengthFilePath = outputDirPath.resolve("word_length_distribution")
    removeOldOutput(wordCountsByLengthFilePath)

    wordLengthDF
      .groupBy($"word_length")
      .count
      .sort($"word_length".desc)
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(wordCountsByLengthFilePath.toString)

    val wordsByLengthFilePath = outputDirPath.resolve("words_by_length")
    removeOldOutput(wordsByLengthFilePath)

    wordLengthDF
      .sort($"word_length".desc, $"word")
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .save(wordsByLengthFilePath.toString)

    val charCountDF = df
      .flatMap(_.split(""))
      .map((_, 1))
      .withColumnRenamed("_1", "char")
      .withColumnRenamed("_2", "count")
      .groupBy($"char")
      .count

    val charCountFilePath = outputDirPath.resolve("char_count")
    removeOldOutput(charCountFilePath)

    charCountDF
      .sort($"char")
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .save(charCountFilePath.toString)

    val charsByFrequencyFilePath = outputDirPath.resolve("char_distribution")
    removeOldOutput(charsByFrequencyFilePath)

    charCountDF
      .sort($"count".desc, $"char")
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("csv")
      .option("sep", "\t")
      .option("quote", "")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .save(charsByFrequencyFilePath.toString)

    println("The whole processing completed in %.5f seconds".format((System.currentTimeMillis() - startTime) / 1000f))

    spark.close
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2 || args.contains("--help") || args.contains("-h")) {
      printUsage()
      System.exit(1)
    }

    try {
      explore(args(0), args(1))
    } catch {
      case ex: Exception => println(ex.getMessage)
        System.exit(1)
    }
  }
}
