package linegroup3.tweetstream.test;

import java.sql.Timestamp;

public class DateTest {

	public static void test(){
		Timestamp s = Timestamp.valueOf("2011-09-01 00:00:00.0");
		Timestamp e = Timestamp.valueOf("2012-05-01 00:00:00.0");
		
		double wholeDay = 24*60*60*1000;
		
		System.out.println((e.getTime() - s.getTime())/wholeDay);
	}
	
}
