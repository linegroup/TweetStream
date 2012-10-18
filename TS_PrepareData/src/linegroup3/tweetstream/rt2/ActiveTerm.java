package linegroup3.tweetstream.rt2;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

public class ActiveTerm {
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
	
	public ActiveTerm(){
		
	}
	
	public ActiveTerm(long timeInterval){
		this.timeInterval = timeInterval;
	}
	
	public boolean isActive(int id){
		for(TermTimePair pair : queue){
			if(pair.id == id)
				return true;
		}
		return false;
	}
	
	public Set<Integer> activeTerms(){
		Set<Integer> ret = new TreeSet<Integer>();
		for(TermTimePair pair : queue){
			ret.add(pair.id);
		}
		return ret;
	}
	
	
	public void active(int id, Timestamp currentTime){
		Timestamp boundTime = new Timestamp(currentTime.getTime() - timeInterval);
		
		TermTimePair pair = queue.peek();
		while(pair != null && pair.t.before(boundTime) ){
			queue.poll();
			pair = queue.peek();
		}
		
		queue.offer(new TermTimePair(id, currentTime));
	}

	
	public class TermTimePair {
		public int id;
		public Timestamp t;
		
		public TermTimePair(int id, Timestamp t){
			this.id = id;
			this.t = t;
		}
		
		
	}
}
