# corpus-explorer

A tool for getting a quick insight in a text corpus.<br>

#### Prerequisites
* Oracle JDK 8 or openjdk-8-jdk
* Apache Spark 2.3.3
* Scala 2.11

#### Usage example
spark-submit --class com.tilde.spark.CorpusExplorer --master local[*] corpus-explorer-1.0-SNAPSHOT.jar ${path/to/plaintext/corpus/file}