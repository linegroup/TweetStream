package linegroup3.tweetstream.onlinelda;

public class ThreeSigmaMonitor {
	
	public ThreeSigmaMonitor(int cycle, int startup){
		CYCLE = cycle;
		queue = new double[CYCLE];
		this.STARTUP = startup;
	}
	
	public boolean add(double v){// return true if it is an anomaly
		if(index < CYCLE){
			queue[index] = v;
			index ++;
			return false;
		}else{ // full
			n ++;
			
			double old_v = queue[index % CYCLE];
			double d = v - old_v;
			s += d;
			s2 += d * d;
			
			if(n <= STARTUP){
				queue[index % CYCLE] = v;
				index ++;
				return false;
			}
			
			if((d - mean()) > 3*sVar()){
				queue[index % CYCLE] = v;
				index ++;
				return true;
			}
		}
		
		queue[index % CYCLE] = v;
		index ++;
		return false;
	}
	
	private int STARTUP = 0;
	private int CYCLE = 0;
	private double[] queue; // for historical data
	private int index = 0;
	
	private int n = 0;
	private double s = 0;
	private double s2 = 0;
	
	private double mean(){
		return s / n;
	}
	
	private double sVar(){
		double e1 = mean();
		double var = s2 / n - e1*e1;
		return Math.sqrt(var);
	}

}
