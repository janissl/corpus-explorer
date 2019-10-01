# corpus-explorer

A tool for getting a quick insight in a text corpus.<br>

#### Prerequisites
* Oracle JDK 8 or openjdk-8-jdk
* Apache Spark 2.3.3
* Scala 2.11

Before running this app, your text corpus must be segmented - one sentence per line. Otherwise, you will get statistics about paragraphs instead of sentences.

#### Usage example
`spark-submit --class com.tilde.spark.CorpusExplorer --master local[*] corpus-explorer-1.0-SNAPSHOT.jar ${path/to/plaintext/corpus/file}`

#### Results
Once the execution of the command above has been completed, you will find the following folders next to your corpus file:
- _*.char_count_
  
  Includes a CSV file with two columns - __character__ and __number_of_its_occurrences__ - sorted by character in alphabetical order.
  
- _*.char_distribution_

  Includes a CSV file with same data as under _*.char_count_ but sorted by the number of occurrences in descending order.
  
- _*.fullstop_tokens_

  Includes a CSV file with two columns - __last_dot_terminated_token_in_sentence__ and __number_of_its_occurrences__ - sorted by the number of occurrences in descending order.
  May be useful for finding erroneous sentence breaks (e.g. after abbreviations) etc.

- _*.sentence_length_distribution_

  Includes a CSV file with two columns - __sentence_length__ and __number_of_sentences_with_such_length__ - sorted by sentence length in descending order.
  
- _*.sentences_by_length_

  Includes a CSV file with two columns - __sentence__ and __sentence_length__ - sorted by sentence length in descending order.
  
- _*.word_count_

  Includes a CSV file with two columns - __word__ and __count__ - sorted by word in alphabetical order.
  
- _*.word_length_distribution_

  Includes a CSV file with two columns - __word_length__ and __number_of_words_with_such_length__ - sorted by word length in descending order.
  
- _*.words_by_length_

  Includes a CSV file with two columns - __word__ and __word_length__ - sorted by word length in descending order.
  
The app will also output a total number of sentences, a number of unique sentences as well as a total number of words, a number of unique words and a total time of execution in seconds in the terminal window.
