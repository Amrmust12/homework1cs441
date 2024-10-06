// Ameer Mustafa
// This code was provided by the Homework Description of a simple map / reduce model that preforms word counts
// Any modified code will be shown with a comment next to it explaining why the change was made to full fill hw1 requirements
// I modified the code to use predefined path for input (input.txt) and specified the name of the output file as well (output.txt)
// I also modified line 44 since from conf.setMapperClass(classOf[Map]) to conf.setMapperClass(classOf[MapReduceProgram.Map]) to work properly

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, EncodingType}




object MapReduceProgram:
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      val line: String = value.toString
      line.split(" ").foreach { token =>
        val cleanedToken = token.replaceAll("[^a-zA-Z]", "") // Cleans the token for anything that is not lower case a-z and A-Z. Meaning any symbol will be cleaned
        if (cleanedToken.nonEmpty) { // Check if token is not empty after removing punctuation
          word.set(cleanedToken)
          output.collect(word, one)
        }
      }
    }


class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
  override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
    val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
    output.collect(key,  new IntWritable(sum.get()))

@main def runMapReduce() =
   val conf: JobConf = new JobConf(this.getClass)
    val inputPath = "src/main/input.txt"
    val outputPath = "src/main/output" // Creates a folder called output file and within the folder part 0000 has all the data results

    conf.setJobName("WordCount")
    conf.set("fs.defaultFS", "local")
    //conf.set("mapreduce.job.maps", "1") Commented to allow Hadoop to decide amount of optimal mappers
    //conf.set("mapreduce.job.reduces", "1") Commented to allow Hadoop decide amount of reducers needed
    conf.setOutputKeyClass(classOf[Text])
    conf.set("dfs.block.size", "64MB") // Setting the amount of data per shard
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MapReduceProgram.Map]) // Adjusted
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)