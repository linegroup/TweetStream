package linegroup3.common;

import java.sql.Timestamp;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;




public class Config {
	static public Configuration config = null;
	
	/*
	static{
		try {
			config = new PropertiesConfiguration("./config.properties");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}*/
	//////////////////////////////// Information ///////////////////////////////////
	
	
	static public int N = 300;
	
	/////////// FetcherMS
	static public Timestamp FetcherMS_start = Timestamp.valueOf("2010-06-05 00:00:00");
	static public Timestamp FetcherMS_end = Timestamp.valueOf("2010-06-13 00:00:00");
	static public String FetcherMS_db = "tweetstream";
	static public String FetcherMS_table = "stream_2010_06";
	
	/////////// Detection time
	static public Timestamp detectionT = Timestamp.valueOf("2010-06-07 00:00:00");
	
	static public double ANOMALY_THRESHOLD = 4.0;
	
	static public long smooth_1 = 15;
	static public long smooth_2 = 5;
	
	
	
	static public String feature = "V"; //"V", "V2", "A", "VA"
	
	
	static public boolean self_set_distribution = true;
	
	static public boolean include_RT = false;
	
	static public double THRESHOLD_D_V = -100.0;
	
	static public double THRESHOLD_D_A = -100.0;
	
	static public Timestamp historyS = Timestamp.valueOf("2010-06-01 00:00:00");
	static public Timestamp historyE = Timestamp.valueOf("2010-06-07 00:00:00");
	static public Timestamp historyStart = Timestamp.valueOf("2010-06-03 00:00:00");
	
	static public void printinfo(){
		System.out.println("N\t" + N);
		System.out.println("FetcherMS_start\t" + FetcherMS_start);
		System.out.println("FetcherMS_end\t" + FetcherMS_end);
		System.out.println("FetcherMS_db\t" + FetcherMS_db);
		System.out.println("FetcherMS_table\t" + FetcherMS_table);
		System.out.println("detectionT\t" + detectionT);
		System.out.println("ANOMALY_THRESHOLD\t" + ANOMALY_THRESHOLD);
		System.out.println("smooth_1\t" + smooth_1);
		System.out.println("smooth_2\t" + smooth_2);
		System.out.println("feature\t" + feature);
		System.out.println("self_set_distribution\t" + self_set_distribution);
		System.out.println("include_RT\t" + include_RT);
		System.out.println("THRESHOLD_D_V\t" + THRESHOLD_D_V);
		System.out.println("THRESHOLD_D_A\t" + THRESHOLD_D_A);
		System.out.println("historyS\t" + historyS);
		System.out.println("historyE\t" + historyE);
		System.out.println("historyStart\t" + historyStart);
	}
}
