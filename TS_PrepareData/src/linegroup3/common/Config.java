package linegroup3.common;

import java.sql.Timestamp;

import linegroup3.experiment.ExperimentAgent;

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
	static public Timestamp historyE = Timestamp.valueOf("2010-07-01 00:00:00");
	static public Timestamp historyStart = Timestamp.valueOf("2010-06-03 00:00:00");
	
	/*
	static public boolean cos_sim = false;	
	static public double SIMILARITY_THRESHOLD = 0.01;
	static public double JOIN_THRESHOLD = 0.0025;
	*/
	static public boolean cos_sim = true;	
	static public double SIMILARITY_THRESHOLD = 0.8;
	static public double JOIN_THRESHOLD = 0.7;
	
	
	static public boolean set_dis_in_add = true;
	static public boolean set_dis_in_getkeywords = false;
	
	static public boolean save_bursts = true;
	
	static public long test_id = 1;
	
	static public void printinfo(){
		System.out.println(parameters());
		if(save_bursts){
			ExperimentAgent.saveParameters(test_id, parameters());
			ExperimentAgent.createTable(test_id);
		}
	}
	
	static public String parameters(){
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("N\t" + N + "\n");
		sb.append("FetcherMS_start\t" + FetcherMS_start + "\n");
		sb.append("FetcherMS_end\t" + FetcherMS_end + "\n");
		sb.append("FetcherMS_db\t" + FetcherMS_db + "\n");
		sb.append("FetcherMS_table\t" + FetcherMS_table + "\n");
		sb.append("detectionT\t" + detectionT + "\n");
		sb.append("ANOMALY_THRESHOLD\t" + ANOMALY_THRESHOLD + "\n");
		sb.append("smooth_1\t" + smooth_1 + "\n");
		sb.append("smooth_2\t" + smooth_2 + "\n");
		sb.append("feature\t" + feature + "\n");
		sb.append("self_set_distribution\t" + self_set_distribution + "\n");
		sb.append("include_RT\t" + include_RT + "\n");
		sb.append("THRESHOLD_D_V\t" + THRESHOLD_D_V + "\n");
		sb.append("THRESHOLD_D_A\t" + THRESHOLD_D_A + "\n");
		sb.append("historyS\t" + historyS + "\n");
		sb.append("historyE\t" + historyE + "\n");
		sb.append("historyStart\t" + historyStart + "\n");
		sb.append("cos_sim\t" + cos_sim + "\n");
		sb.append("SIMILARITY_THRESHOLD\t" + SIMILARITY_THRESHOLD + "\n");
		sb.append("JOIN_THRESHOLD\t" + JOIN_THRESHOLD + "\n");
		sb.append("set_dis_in_add\t" + set_dis_in_add + "\n");
		sb.append("set_dis_in_getkeywords\t" + set_dis_in_getkeywords + "\n");
		sb.append("test_id\t" + test_id + "\n");
		
		return sb.toString();
	}
}
