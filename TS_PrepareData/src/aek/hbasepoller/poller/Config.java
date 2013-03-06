package aek.hbasepoller.poller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Config {
	private static String CONFIG_FILE = "conf/config.properties";
	private static Properties prop;
	
	public static void initConfig(){
		try {
			prop = new Properties();
			prop.load(new FileInputStream(new File(CONFIG_FILE)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void initConfig(String configFile){
		try {
			prop = new Properties();
			prop.load(new FileInputStream(new File(configFile)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static String getParameter(String key){
		if(prop == null){
			initConfig();
		}
		if(prop.getProperty(key) != null){
			return prop.getProperty(key).trim();
		}
		return null;
	}
}
