

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.map.HashedMap;

public class TweetParsingUtils {
	// You should filter the words from the array bellow.
	public static String [] forbiddenWords = {"be", "for", "is", "and", "of", "in", "a", "to", "the", "with", "that", "on", "at", "with", "this", "are", "will"};
	
	public static HashMap<String, String> getAuthorAndTweetFromCSV(String text) {
		Pattern pattern = Pattern.compile("\\w+\\s*,\\s*(\\w+)\\s*,\\s*\"*(.+?)\"*\\s*,\\s*.*", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(text);	
		
		HashMap<String, String> columns = new HashMap<>(2);
		
		if(matcher.find()) {
			columns.put("author", matcher.group(1));
			columns.put("tweet", matcher.group(2));
		}
		
		return columns;
	}
	
	public static String [] breakTweetIntoWords(String tweet) {
		String [] words = tweet.split("[\\s*]");
		for(int i = 0; i < words.length; i++) {
			words[i] = words[i].replaceAll("[\\W&&[^#]]", "").toLowerCase();
		}
		return words;
	}
}
