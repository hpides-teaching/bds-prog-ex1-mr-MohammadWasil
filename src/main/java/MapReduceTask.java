import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class MapReduceTask {

//    Map function for counting the appearances of each word in the tweet corpus.
    public static class WordExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {
 	private Text w = new Text();
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
        	
			if(!tweet.equals("")) {
				String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
   			        
				for(int i =0; i <= words.length ; i++){
				    //System.out.println(words[i]+ " 1");
				    w.set(words[i]);
				    context.write(w, new IntWritable(1));
				}
			}
			//context.write(w, new IntWritable(1));
        }
    };

//  Reduce function for aggregating the number of appearances of each word in the tweet corpus.
    public static class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        	
        	int sum = 0;
        	for (IntWritable value : values){
        		sum = sum + value.get();
        	}
       	
		result.set(sum);
        	
        	context.write(key, result);
        }
        
    }

    /*
     *
     * Here you should write the map, reduce and all other helper methods for
     * solving the other 3 tasks. Observe that the tweet parser and other utility methods
     * are available in TweetParsingUtils.java
     *
     * */



    /* Method for setting up and executing the word count Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences of each word in the tweets. */
    public void wordCount(String inputWordCount, String outputWordCount) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job wc = Job.getInstance(conf1, "word-count");

        wc.setJarByClass(MapReduceTask.class);
        wc.setMapperClass(WordExtractorMapper.class);
        wc.setReducerClass(WordCounterReducer.class);

        wc.setOutputKeyClass(Text.class);
        wc.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wc, new Path(inputWordCount));
        FileOutputFormat.setOutputPath(wc, new Path(outputWordCount));

        wc.setInputFormatClass(TextInputFormat.class);
        wc.setOutputFormatClass(TextOutputFormat.class);
        wc.waitForCompletion(true);
    }

    /* Method for setting up and executing the top word Hadoop job (most used word in the tweets). This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences of the most used word in the tweets. */
    public void topWord(String inputWordCount, String outputTop1Word) throws InterruptedException, IOException, ClassNotFoundException {

    }

    /* Method for setting up and executing the Donald Trump's top 10 words Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used words by Donald Trump. */
    public void top10TrumpWords(String inputWordCount, String outputTop10TrumpWords) throws IOException, ClassNotFoundException, InterruptedException {

    }

    /* Method for setting up and executing the Donald Trump's top 10 hashtags Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used hashtags by Donald Trump. */
    public void top10TrumpHashtags(String inputWordCount, String outputTop10TrumpHashtags) throws IOException, ClassNotFoundException, InterruptedException {

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String inputWordCount = args[0];
        String outputWordCount = args[1];
        String outputTop1Word = args[2];
        String outputTop10TrumpWords = args[3];
        String outputTop10TrumpHashtags = args[4];

        MapReduceTask mrt = new MapReduceTask();

        mrt.wordCount(inputWordCount, outputWordCount);
        mrt.topWord(inputWordCount, outputTop1Word);
        mrt.top10TrumpWords(inputWordCount, outputTop10TrumpWords);
        mrt.top10TrumpHashtags(inputWordCount, outputTop10TrumpHashtags);
    }
}