package linegroup3.tweetstream.preparedata;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import cmu.arktweetnlp.Twokenize;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//LoadTweetsInFile.statisticMonth();
		
		//StatisticTweets.roughAnalysisPerWeek2();
		
		//Preprocess.doCounting();
		
		//ConstructUserTable.doJob();
		
		/*
		String str = "   	  \trt @::.1!!!!2ds\"fds\"fsd,3";
		String[] terms = str.split("[\\s,.!():\"]+");
		for(String term : terms){
			System.out.println(term);
		}
		*/

		/*
		List<String> terms = Twokenize.tokenizeRawTweetText(";;;;;;;;;;;;;;;........................................................(");
		for(String term : terms){
			if(!term.matches("\\p{Punct}+"))
				System.out.println(term);
		}
		*/
		
		for(long i = 1; i < 1000; i ++){
			System.out.println("" + i + "\t:\t" + new Long(i).hashCode() % 1000);
		}
		
		
		/*
		String str1 = "hellös";
		String str2 = "hellös";
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put(str1, 1);
		map.put(str2, 2);
		
		System.out.println(map.size());
		*/
		
		
	}

}
