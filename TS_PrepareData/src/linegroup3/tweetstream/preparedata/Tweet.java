package linegroup3.tweetstream.preparedata;

import java.sql.Timestamp;

public class Tweet {
	public String user_ID;
	public String status_ID; 
	public String twt;
	public Timestamp t;
	
	public Tweet(String user_ID, String status_ID, String twt, Timestamp t){
		this.user_ID = user_ID;
		this.status_ID = status_ID;
		this.twt = twt;
		this.t = t;
	}
	
	
}
