package linegroup3.common;

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
}
