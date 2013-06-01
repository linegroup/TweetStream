package linegroup3.tweetstream.preparedata;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.configuration.BaseConfiguration;

import linegroup3.common.Config;
import linegroup3.tweetstream.event.DBAgent;
import linegroup3.tweetstream.inference.ParallelInfer;
import linegroup3.tweetstream.postprocess.BatchInference;
import linegroup3.tweetstream.postprocess.BatchInferenceForBaseline;
import linegroup3.tweetstream.postprocess.InferenceEfficiency;
import linegroup3.tweetstream.postprocess.ProcessSketch;
import linegroup3.tweetstream.postprocess.ProcessSpeedLog;
import linegroup3.tweetstream.postprocess.SigmaTest;
import linegroup3.tweetstream.postprocess.SketchCheck;
import linegroup3.tweetstream.postprocess.SketchEfficiencyTest;
import linegroup3.tweetstream.postprocess.Statistics;
import linegroup3.tweetstream.postprocess.TokenizeStream;
import linegroup3.tweetstream.postprocess.WordsStastics;
import linegroup3.tweetstream.preparedata.othermodel.RandomChooseUsers;
import linegroup3.tweetstream.rt2.*;
import linegroup3.tweetstream.test.DateTest;
import linegroup3.tweetstream.trend.GetTrendingTopics;

import cmu.arktweetnlp.Twokenize;

public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		
		//LoadTweetsInFile.statisticMonth();
		
		//StatisticTweets.roughAnalysisPerWeek2();
		
		//Preprocess.doCounting();
		
		//ConstructUserTable.doJob();
		
		
		//Speed.doJob();
		//Speed.showDSpeed(Timestamp.valueOf("2011-10-03 00:00:00"), Timestamp.valueOf("2011-10-06 23:00:00"));
		/*
		double data[] = new double[30];

		data[15] = 1;
		data[24] = 3;
		data[25] = 10;
		data[26] = 3;
		
		
		ThreeSigmaMonitor monitor = new ThreeSigmaMonitor(10, 10);
		for(int i = 0; i < 30; i ++){
			if(monitor.add(data[i])){
				System.out.println(i);
			}
		}*/
		
		/*
		if(Config.config == null){
			Config.config = new BaseConfiguration();
			
			if(args.length < 10){
				System.out.println("Usage: -s \"2011-12-13 00:00:00.0\" -e \"2011-12-16 00:00:00.0\" -gap 15 -nTopic 100 -db test (optional)-debug true");
				return;
			}
			
			Timestamp s = Timestamp.valueOf(args[1]);
			Config.config.setProperty("s", s);
			Timestamp e = Timestamp.valueOf(args[3]);
			Config.config.setProperty("e", e);
			int gap = Integer.parseInt(args[5]);
			Config.config.setProperty("gap", gap);
			int nTopic = Integer.parseInt(args[7]);
			Config.config.setProperty("nTopic", nTopic);
			Config.config.setProperty("db", args[9]);
			
			if(args.length >= 12){
				Config.config.setProperty("debug", Boolean.parseBoolean(args[11]));
			}
			
			
			linegroup3.tweetstream.onlinelda.RTProcess.runTime(s, e,	null, gap* 60 * 1000, nTopic);
		}else{
			Timestamp s = Timestamp.valueOf(Config.config.getString("s"));
			Timestamp e = Timestamp.valueOf(Config.config.getString("e"));
			int nTopic = Config.config.getInt("nTopic");
			int gap = Config.config.getInt("gap");
			
			linegroup3.tweetstream.onlinelda.RTProcess.runTime(s, e,	null, gap* 60 * 1000, nTopic);
		}
	*/
		
		
		
		//new RTProcess().runTime(Timestamp.valueOf("2011-09-01 00:00:00.0"), Timestamp.valueOf("2012-05-01 00:00:00.0"));
		//new RTProcess().runTime(Timestamp.valueOf("2011-10-05 15:00:00.0"), Timestamp.valueOf("2011-10-07 00:00:00.0"));
		//ProcessSpeedLog.trimD("D:/data_for_release2/data");
		//SigmaTest.test("D:/data_for_release2/data");
		//new ParallelInfer().infer();
		//RandomChooseUsers.choose();
		//TestHash.doJob();
		
		//ProcessSketch.processFirstOrder("D:/data_for_release2/data/sketch");
		
		/*
		for(int i = 1; i < 10000000; i ++)
			TestHash.test1(i);
		*/
		
		
		//DistributionTest.test2(100000);

		
		//TransferWords.transfer();
		
		//DateTest.test();
		
		
		//GetTrendingTopics.run();
		//WordsStastics.runTime(Timestamp.valueOf("2011-01-01 00:00:00.0"), Timestamp.valueOf("2012-01-01 00:00:00.0"));
		//new SketchEfficiencyTest().runTime(Timestamp.valueOf("2011-01-01 00:00:00.0"), Timestamp.valueOf("2011-01-02 00:00:00.0"));
		
		
		//new BatchInference().uniInfer("D:/share/data_2011_12/sketch/2011_12_15_03_33_46_0/");
		//new BatchInference().batchInfer("D:/share/stream/");
		//new BatchInference().batchInfer("D:/share/newstream2/");
		//new BatchInference().uniInfer("D:/share/data/sketch/2011_12_15_04_33_27_0/");
		//new BatchInference().batchInfer("D:/share/data/sketch/");
		//new BatchInference().uniInfer("D:/share/data_2011_12/sketch/2011_12_15_03_33_46_0/");
		//new BatchInference().batchInfer("D:/share/test_no_omg/");
		//new BatchInference().batchInfer("D:/share/data_2011_3_months_threshold_10/sketch/");
		
		//new BatchInference().batchInfer("D:/share/data_2011_3_months_threshold_10/sketch/");
		
		
		
		//new BatchInference().uniInfer("D:/data/2011_12_16_20_20_59_0/");
		
		//new BatchInference().batchInfer("D:/share/data_2011_12/sketch/");
		//new BatchInference().batchInfer("D:/share/data_2011_12_more_stop_words/sketch/");
		//new BatchInference().batchInfer("D:/share/test_more_stop_words/");
		//new BatchInference().batchInfer("D:/share/data_add_ok/sketch/");
		//new BatchInference().batchInfer("D:/share/test_newstream2/");
		//new BatchInference().batchInfer("D:/share/data_2011_3_months/sketch/");
		
		
		//new BatchInferenceForBaseline().batchInfer("D:/share/data_2011_12_more_stop_words/sketch/");
		
		
		//TokenizeStream.tokenize();
		
		//SketchCheck.checkBatch("D:/share/data_2011_12/sketch/");
		
		//Statistics.job1();
		
		//ProcessSpeedLog.processD("D:/share/log_2011_12");
		//ProcessSpeedLog.print("D:/share/data_smrt_threshold_10");
		
		
		
		//DBAgent.processEvents_baseline();
		//DBAgent.processEvents();
		//DBAgent.testBurstOnTwoStateMachine();
		//DBAgent.testBurstOnTwoStateMachine_baseline();
		//DBAgent.testBurstOnTwoStateMachine_minute();
		
		/////////////////////////////////////////////////////////////////
		/*
		List<String> ret = new LinkedList<String>();
		
		//String str = "???????????????????????????????????????? abc";
		String str = "?????????????????????????????????????????????????????????????? abc ?????????????";
		str = str.replaceAll("\\.{10,}+", " ");
		List<String> terms = Twokenize.tokenize(str);

		
		final String regex = "\\p{Punct}+";
		for (String term : terms) {
			if (term.length() > 0 && term.length() <= 64
					&& !term.matches(regex)) {
				ret.add(term);
			}
		}
		
		
		System.out.println();
		*/
		
		
		/////////////////////////////////////////////////////////////////
		
		
		
		//System.out.println(HashFamily.hash(4, "@dorothy1410"));
		//System.out.println(HashFamily.hash(4, "news"));
		
		/*
		System.out.println(HashFamily.hash(4,"@zentyh".hashCode()));
		System.out.println(HashFamily.hash(4,"#rip".hashCode()));
		*/
		
		
		
		
		//new InferenceEfficiency().infer();
		
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
		
		/*
		for(long i = 1; i < 1000; i ++){
			System.out.println("" + i + "\t:\t" + new Long(i).hashCode() % 1000);
		}
		*/
		
		
		/*
		String str1 = "hellös";
		String str2 = "hellös";
		Map<String, Integer> map = new TreeMap<String, Integer>();
		map.put(str1, 1);
		map.put(str2, 2);
		
		System.out.println(map.size());
		*/
		
		//linegroup3.tweetstream.onlinelda.Test.test();
		
		//new RTProcess2().runTime(Timestamp.valueOf("2011-09-25 00:00:00.0"), Timestamp.valueOf("2012-01-01 00:00:00.0"), Timestamp.valueOf("2011-09-30 23:59:59"));
		//new RTProcess2().runTime(null);
		new RTProcessOffline().runTime(Config.detectionT);
	}
	
	/*
	static public long hash64shift(long key)
	{
	  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
	  key = key ^ (key >>> 24);
	  key = (key + (key << 3)) + (key << 8); // key * 265
	  key = key ^ (key >>> 14);
	  key = (key + (key << 2)) + (key << 4); // key * 21
	  key = key ^ (key >>> 28);
	  key = key + (key << 31);
	  return key;
	}
	
	*/
	
	
	

}
