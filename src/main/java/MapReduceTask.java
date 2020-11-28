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
// import statement I added.
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Collection;
import java.util.*;

public class MapReduceTask {


//   1. Map function for counting the appearances of each word in the tweet corpus.
    public static class WordExtractorMapper extends Mapper<Object, Text, Text, IntWritable> {
 	
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
        	
			if(!tweet.equals("")) {
				String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
   			        
   			        for( int i = 0; i < words.length ; i++ )
   			        {
					   			        
   				        // To check if the key is not present in the array forbidden words.
					if(!checkElement(words[i]))
					{
						context.write(new Text(words[i]), new IntWritable(1) );
					}
   			        }
			}
			//context.write(w, new IntWritable(1));
        }
        
        protected static boolean checkElement(String keys)
    	{
		for(String word : TweetParsingUtils.forbiddenWords)
		{
		    if(word.equals(keys))
		    {
		        return true;
		    }
		}
		return false;
    	}
        
    };

//  Reduce function for aggregating the number of appearances of each word in the tweet corpus.
    public static class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable count = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        	
        	Iterator<IntWritable> iter = values.iterator();
        	int sum=0;
        	while(iter.hasNext()){
        		IntWritable value_ = iter.next();
        		sum += value_.get();
        	}
        	count.set(sum);
        	context.write(key, count);
        }
    }

    /*
     *
     * Here you should write the map, reduce and all other helper methods for
     * solving the other 3 tasks. Observe that the tweet parser and other utility methods
     * are available in TweetParsingUtils.java
     *
     * */
    
//  2. Map function for counting top words.
    public static class TopWordMapper extends Mapper<Object, Text, Text, IntWritable> {
 	
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
	
		if(!tweet.equals("")) {
			
			String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
			
			for (int i=0; i< words.length; i++)
			{
				// To check if the key is not present in the array forbidden words.
				if(!checkElement(words[i]))
				{
					context.write(new Text(words[i]), new IntWritable(1) );
				}
			}
		}
	}    
	
	protected static boolean checkElement(String keys)
    	{
		for(String word : TweetParsingUtils.forbiddenWords)
		{
		    if(word.equals(keys))
		    {
		        return true;
		    }
		}
		return false;
    	}
    }
    
//  Reduce function for aggregating the top words in the tweet corpus.
    public static class TopWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
	private Map<String,Integer> wordHashMap = new HashMap<String, Integer>();
    
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		
		// Calculate the count of the words.
		Iterator<IntWritable> iter = values.iterator();
		int sum=0;
		while(iter.hasNext())
		{
			IntWritable value_ = iter.next();
			sum += value_.get();	
		}
		
		// Store the word-count pair of each words into hashmap
		if( wordHashMap.containsKey(key))
		{
			wordHashMap.put(key.toString(), sum);
		}
		else
		{
			wordHashMap.putIfAbsent(key.toString(), sum);		
		}
		     
        }
        
        
	protected void cleanup(Context context) throws IOException, InterruptedException{
		// Here, we will take the maximum value from the hashmap, and use this value to
		// compare and get their corresponding key.
		
		// Get the maximum value from the hashmap
		Collection<Integer> value_ = wordHashMap.values();
        	int maxValue_ = Collections.max(value_);
        	
        	// Iterate through the hashmap
        	for (Map.Entry<String, Integer> e : wordHashMap.entrySet() )
        	{
        		if(e.getValue() == maxValue_)
        		{
				// write the word and their count.
        			context.write(new Text(e.getKey()), new IntWritable(e.getValue() ));
        		}
		}
	}
    }
    
// 3.  Map function for counting the top 10 words used by Mr. Donald Trump.
   public static class TrumpWordsMapper extends Mapper<Object, Text, Text, IntWritable> {

	private String authorDonald = "realDonaldTrump"; 	
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
        	
		if(!tweet.equals("")) {
			// change here.
			if(author.equals(authorDonald) ){
			
				String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
   			        
   			        for( int i = 0; i < words.length ; i++ )
   			        {
   			        	 // To check if the key is not present in the array forbidden words.
					if(!checkElement(words[i]))
					{
	   			        	//context.write(new Text(words[i]), new IntWritable(1) );
	   			        	context.write(new Text(words[i]), new IntWritable(1) );
   			        	}
   			        }
			}				
		}
		//context.write(w, new IntWritable(1));
        }
        
        protected static boolean checkElement(String keys)
    	{
		for(String word : TweetParsingUtils.forbiddenWords)
		{
		    if(word.equals(keys))
		    {
		        return true;
		    }
		}
		return false;
    	}
    }

    //  Reduce function for aggregating the number of top 10 words used by Mr. Donald Trump.
    public static class TrumpWordsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
	private HashMap<String,Integer> wordHashMap = new HashMap<String, Integer>();
    
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	
        	// Calculate the count of the words.
		Iterator<IntWritable> iter = values.iterator();
		int sum=0;
		while(iter.hasNext())
		{
			IntWritable value_ = iter.next();
			sum += value_.get();
		}	
		//count.set(sum);
		//context.write(key, new Text(Integer.toString(sum)));

 	     	// Store the word-count pair of each words into hashmap
		if( wordHashMap.containsKey(key))
		{
			wordHashMap.put(key.toString(), sum);
		}
		else
		{
			wordHashMap.putIfAbsent(key.toString(), sum);		
		}
    	}
    	
    	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		// Colect the top 10 values from the hashmap and their key value.
		int counter = 0;
		Iterator<Map.Entry<String,Integer>> iter = wordHashMap.entrySet().iterator();
		
		loop:
		while (iter.hasNext()){
		    Map.Entry<String,Integer> entry = iter.next();
		    
		    Collection<Integer> value_ = wordHashMap.values();
		    int maxValue_ = Collections.max(value_);
		    
		    
		    if(entry.getValue() == maxValue_){
		        
		        context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		        //System.out.println(entry.getKey() + " " + entry.getValue());
		        iter.remove();
		        
		        iter = wordHashMap.entrySet().iterator();
		        counter ++;
		        if(counter < 10)
		        {
		            continue loop;
		        }
		        else{
		            break;
		        }   
		    }
        	}
	}
    }
    /*
    public static class TrumpWordsMapper2 extends Mapper<Object, Text, Text, Text> {
	
	//private LongWritable key = new LongWritable();
	//private LongWritable val = new LongWritable();
	//private String authorDonald = "realDonaldTrump"; 	
        //@Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

		//Iterator<LongWritable> iter = value.iterator();

		//while(iter.hasNext() )
		//{
			//key = iter.next();
			//String key = val_1.get();
			
		//	val = iter.next();
		//}

		//context.write(value, key);
		
		String [] keyValuePair = value.toString().split("\t");
		String key_ = keyValuePair[0];
		String value_ = keyValuePair[1];		
		
		context.write(new Text(value_), new Text(key_));
		
        }    
    }
    *//*
    //  Reduce function for aggregating the number of top 10 words used by Mr. Donald Trump.
    public static class TrumpWordsReduce2 extends Reducer<Text, Text, Text, Text> {

	//private IntWritable count = new IntWritable();
	//private Map<String,Integer> wordHashMap = new HashMap<String, Integer>();
    
	@Override
        protected void reduce(Text key, Iterable<Text> value,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	
		int count=0;
		for(Text val : value)
		{
			count+=1;				
						
			//
			// Store the word-count pair of each words into hashmap
			
		}

		context.write(key, new Text(Integer.toString(count))) ;
		//context.write(value, key) ;
		

	
	
		//int sum = 0;
        	// Calculate the count of the words.
        	//for(Text value_ : value)
        	//{	
        	//	sum += 1;
        	//}
        	//int sum=0;
        	//String keyes = "";
        	//int sum = 0;
	      	//for ( LongWritable val : value)
        	//{
        //		sum = Integer.parseInt(val.toString().split("\t")[0]);
        //		keyes = value.toString().split("\t")[1];
        //		//count.set(sum);
        //		context.write(new Text(keyes), new IntWritable(sum));
        //	}
	//int count= 0;
        //int amount =0;
        //string market = "";
        //for(IntWritable value : values) {
        //   market = value.toString().split(" ")[1];
        //   amount = Integer.parseInt(value.toString.split(" ")[0])
        //    if(count < 10){
        //      count ++;
        //      context.write(key, value);
        //  }
		//count.set(sum);
        	//context.write(key, value);
   	 }
    }*/
    
    // 4.  Map function for counting the top 10 words used by Mr. Donald Trump.
   public static class TrumpHashtagMapper extends Mapper<Object, Text, Text, IntWritable> {

	private String authorDonald = "realDonaldTrump"; 	
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

        	HashMap<String, String> parsedCsv = TweetParsingUtils.getAuthorAndTweetFromCSV(value.toString());
        	String author = parsedCsv.getOrDefault("author", "");
        	String tweet = parsedCsv.getOrDefault("tweet", "");
        	
		if(!tweet.equals("")) {
			// change here.
			if(author.equals(authorDonald)){
			
				String [] words = TweetParsingUtils.breakTweetIntoWords(tweet);
   			        
   			        for( int i = 0; i < words.length ; i++ )
   			        {
   			        	 // To check if the key is not present in the array forbidden words, and that word starts with "#"
					if(!checkElement(words[i]) && words[i].startsWith("#"))
					{
	   			        	//context.write(new Text(words[i]), new IntWritable(1) );
	   			        	context.write(  new Text(words[i]), new IntWritable(1) );
   			        	}
   			        }
			}				
		}
        }
        
        protected static boolean checkElement(String keys)
    	{
		for(String word : TweetParsingUtils.forbiddenWords)
		{
		    if(word.equals(keys))
		    {
		        return true;
		    }
		}
		return false;
    	}
    }
    
    //  Reduce function for aggregating the number of top 10 hashtags used by Mr. Donald Trump.
    public static class TrumpHashtagsReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private HashMap<String,Integer> wordHashMap = new HashMap<String, Integer>();
    
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
	
        	// Calculate the count of the words.
		Iterator<IntWritable> iter = values.iterator();
		int sum=0;
		while(iter.hasNext())
		{
			IntWritable value_ = iter.next();
			sum += value_.get();
		}	
		//count.set(sum);
		//context.write(key, new Text(Integer.toString(sum)));

 	     	// Store the word-count pair of each words into hashmap
		if( wordHashMap.containsKey(key))
		{
			wordHashMap.put(key.toString(), sum);
		}
		else
		{
			wordHashMap.putIfAbsent(key.toString(), sum);		
		}
    	}
    	
    	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		// Colect the top 10 values from the hashmap and their key value.
		int counter = 0;
		Iterator<Map.Entry<String,Integer>> iter = wordHashMap.entrySet().iterator();
		
		loop:
		while (iter.hasNext()){
		    Map.Entry<String,Integer> entry = iter.next();
		    
		    Collection<Integer> value_ = wordHashMap.values();
		    int maxValue_ = Collections.max(value_);
		    
		    
		    if(entry.getValue() == maxValue_){
		        
		        context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
		        //System.out.println(entry.getKey() + " " + entry.getValue());
		        iter.remove();
		        
		        iter = wordHashMap.entrySet().iterator();
		        counter ++;
		        if(counter < 10)
		        {
		            continue loop;
		        }
		        else{
		            break;
		        }   
		    }
        	}
	}
    }
    
    
    
    
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
    	Configuration conf2 = new Configuration();
    	Job tw = Job.getInstance(conf2, "top-word");
    	
    	tw.setJarByClass(MapReduceTask.class);
    	tw.setMapperClass(TopWordMapper.class);
    	tw.setReducerClass(TopWordReducer.class);
    	
    	tw.setOutputKeyClass(Text.class);
    	tw.setOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.addInputPath(tw, new Path(inputWordCount));
    	FileOutputFormat.setOutputPath(tw, new Path(outputTop1Word));
    	
    	tw.setInputFormatClass(TextInputFormat.class);
	tw.setOutputFormatClass(TextOutputFormat.class);
	tw.waitForCompletion(true);
    }

    /* Method for setting up and executing the Donald Trump's top 10 words Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used words by Donald Trump. */
    public void top10TrumpWords(String inputWordCount, String outputTop10TrumpWords) throws IOException, ClassNotFoundException, InterruptedException {

	Configuration conf3 = new Configuration();
    	Job trumpWord = Job.getInstance(conf3, "trump-word");
    	
    	trumpWord.setJarByClass(MapReduceTask.class);
    	trumpWord.setMapperClass(TrumpWordsMapper.class);
    	trumpWord.setReducerClass(TrumpWordsReduce.class);
    	
  	trumpWord.setMapOutputKeyClass(Text.class);
    	trumpWord.setMapOutputValueClass(IntWritable.class);    	
    	
    	trumpWord.setOutputKeyClass(Text.class);
    	trumpWord.setOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.addInputPath(trumpWord, new Path(inputWordCount));
    	FileOutputFormat.setOutputPath(trumpWord, new Path(outputTop10TrumpWords));
    	
    	trumpWord.setInputFormatClass(TextInputFormat.class);
	trumpWord.setOutputFormatClass(TextOutputFormat.class);
	trumpWord.waitForCompletion(true);
	
	
	/*
	Configuration conf31 = new Configuration();
    	Job trumpWord1 = Job.getInstance(conf31, "trump-word1");
    	trumpWord1.setJarByClass(MapReduceTask.class);
    	trumpWord1.setMapperClass(TrumpWordsMapper2.class);
    	//trumpWord1.setReducerClass(TrumpWordsReduce2.class);
	
    	trumpWord1.setMapOutputKeyClass(Text.class);
    	trumpWord1.setMapOutputValueClass(Text.class);    
    	
    	trumpWord1.setOutputKeyClass(Text.class);    // Text
    	trumpWord1.setOutputValueClass(Text.class);  // Intwritablr
	
	FileInputFormat.addInputPath(trumpWord1, new Path(Intermediate));
    	FileOutputFormat.setOutputPath(trumpWord1, new Path(outputTop10TrumpWords));
    	trumpWord1.waitForCompletion(true);
    	
        //System.exit(trumpWord1.waitForCompletion(true) ? 0 : 1);
	*/

    }

    /* Method for setting up and executing the Donald Trump's top 10 hashtags Hadoop job. This method receives as parameters
     * the path to the csv file containing the tweets and the path to the output file where it must write
     * the number of occurrences for each of the 10 most used hashtags by Donald Trump. */
    public void top10TrumpHashtags(String inputWordCount, String outputTop10TrumpHashtags) throws IOException, ClassNotFoundException, InterruptedException {
	
	Configuration conf4 = new Configuration();
    	Job trumpHashtags = Job.getInstance(conf4, "trump-hashtags");
    	
    	trumpHashtags.setJarByClass(MapReduceTask.class);
    	trumpHashtags.setMapperClass(TrumpHashtagMapper.class);
    	trumpHashtags.setReducerClass(TrumpHashtagsReduce.class);
    	
  	trumpHashtags.setMapOutputKeyClass(Text.class);
    	trumpHashtags.setMapOutputValueClass(IntWritable.class);    	
    	
    	trumpHashtags.setOutputKeyClass(Text.class);
    	trumpHashtags.setOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.addInputPath(trumpHashtags, new Path(inputWordCount));
    	FileOutputFormat.setOutputPath(trumpHashtags, new Path(outputTop10TrumpHashtags));
    	
    	trumpHashtags.setInputFormatClass(TextInputFormat.class);
	trumpHashtags.setOutputFormatClass(TextOutputFormat.class);
	trumpHashtags.waitForCompletion(true);
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
