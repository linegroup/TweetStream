package linegroup3.tweetstream.rt2;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;


public class ActiveTerm2 {
	private final int InitialCapacity = 100000;
	private long timeInterval = 15*60*1000; // default 15 minutes
	
	private PriorityQueue<TermTimePair> queue = new PriorityQueue<TermTimePair>(InitialCapacity, new Comparator<TermTimePair>(){
		@Override
		public int compare(TermTimePair o1, TermTimePair o2) {
			if(o1.t.after(o2.t)) return 1;
			if(o2.t.after(o1.t)) return -1;
			return 0;
		}
	});
	
	public ActiveTerm2(){
		
	}
	
	public ActiveTerm2(long timeInterval){
		this.timeInterval = timeInterval;
	}
	
	public boolean isActive(String term){
		for(TermTimePair pair : queue){
			if(pair.term == term)
				return true;
		}
		return false;
	}
	
	public Set<String> activeTerms(){
		Set<String> ret = new TreeSet<String>();
		for(TermTimePair pair : queue){
			ret.add(pair.term);
		}
		return ret;
	}
	
	
	public void active(String term, Timestamp currentTime){
		Timestamp boundTime = new Timestamp(currentTime.getTime() - timeInterval);
		
		TermTimePair pair = queue.peek();
		while(pair != null && pair.t.before(boundTime) ){
			queue.poll();
			pair = queue.peek();
		}
		
		queue.offer(new TermTimePair(term, currentTime));
	}

	
	public class TermTimePair {
		public String term;
		public Timestamp t;
		
		public TermTimePair(String term, Timestamp t){
			this.term = term;
			this.t = t;
		}
		
		
	}
}
