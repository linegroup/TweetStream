package linegroup3.experiment;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import linegroup3.tweetstream.event.Burst;
import linegroup3.tweetstream.event.BurstCompare;
import linegroup3.tweetstream.event.OnlineEvent;
import linegroup3.tweetstream.io.output.CacheAgent;

public class GenerateEventsFromBursts {

	static List<OnlineEvent> events = new LinkedList<OnlineEvent>();
	
	static private void flush(Timestamp t){
		Timestamp deadline = new Timestamp(t.getTime() - BurstCompare.T_GAP);
		
		List<OnlineEvent> ret = new LinkedList<OnlineEvent>();
		
		for(OnlineEvent event : events){
			CacheAgent.get().update(event.getId(), event);
			if(event.getEnd().before(deadline)){
				System.out.println("flush:\t" + event);
			}else{
				ret.add(event);
			}
		}	
		
		events = ret;
	}
	
	static void process(Timestamp t, List<Burst> bursts){
		for(Burst burst : bursts){
			System.out.println(burst.toString());
		}
		BurstCompare.join(events, bursts);
		flush(t);
	}
	
	static void endProcess(){
		flush(Timestamp.valueOf("2100-01-01 00:00:00"));
	}
	
}
