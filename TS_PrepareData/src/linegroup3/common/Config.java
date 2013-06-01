package linegroup3.common;

import java.sql.Timestamp;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;




public class Config {
	static public Configuration config = null;
	
	static{
		try {
			config = new PropertiesConfiguration("./config.properties");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
	}
	
	static public int N = 300;
	
	/////////// FetcherMS
	static public Timestamp FetcherMS_start = Timestamp.valueOf("2010-06-02 00:00:00");
	static public Timestamp FetcherMS_end = Timestamp.valueOf("2010-06-13 00:00:00");
	static public String FetcherMS_db = "tweetstream";
	static public String FetcherMS_table = "stream_2010_06";
	
	/////////// Detection time
	static public Timestamp detectionT = Timestamp.valueOf("2010-06-07 00:00:00");
	
	
}
