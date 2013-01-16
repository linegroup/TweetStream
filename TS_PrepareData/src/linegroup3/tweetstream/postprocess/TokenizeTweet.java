package linegroup3.tweetstream.postprocess;

import java.util.LinkedList;
import java.util.List;

import cmu.arktweetnlp.Twokenize;

import linegroup3.tweetstream.rt2.StopWords;

public class TokenizeTweet {

	/**
	 * @param args
	 */
	public static List<String> tokenizeTweet(String tweet){
		tweet = decode(tweet);
		tweet = downcase(tweet);
		List<String> terms = tokenize(tweet);
		
		List<String> finalTerms = new LinkedList<String>();
		for(String term : terms){
			if(!StopWords.isStopWord(term)){
				finalTerms.add(term);
			}		
		}
		
		return finalTerms;
	}

	
	private static String decode(String tweet){
		return org.apache.commons.lang.StringEscapeUtils.unescapeHtml(tweet);
	}
	
	private static String downcase(String tweet) {
		return tweet.toLowerCase();
	}
	
	private static List<String> tokenize(String tweet) {
		List<String> ret = new LinkedList<String>();
		
		String str = tweet;
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		
		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}
		

		return ret;

	}
}