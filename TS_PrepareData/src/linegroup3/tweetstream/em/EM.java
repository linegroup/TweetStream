package linegroup3.tweetstream.em;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class EM implements UnitProcessor{
	Batch batch = null;
	ArrayList<Map<String, Double>> theta0 = null;
	double[] p0 = null;
	ArrayList<Map<String, Double>> theta1 = null;
	double[] p1 = null;
	int K = 0;
	
	
	public void learn(Batch batch_, int K_, int N, int iterN){
		// K is the number of topics, N is the estimation of the number of words, iterN is the number of iterations
		
		
		// initialization
		batch = batch_;
		K = K_;
		
		theta0 = new ArrayList<Map<String, Double>>(K);
		for(int k = 0; k < K; k ++){
			theta0.add(new HashMap<String, Double>(N));
		}
		
		p0 = new double[K];
		
		theta1 = new ArrayList<Map<String, Double>>(K);
		for(int k = 0; k < K; k ++){
			theta1.add(new HashMap<String, Double>(N));
		}
		
		p1 = new double[K];
		
		// set theta0 and p0
		set();
		
		// iteration
		for(int i = 0; i < iterN; i ++){
			System.out.println("E = " + E());  // for debug
			
			
			batch.scan(this);
			theta0 =  theta1;
			p0 = p1;
			
			theta1 = new ArrayList<Map<String, Double>>(K);
			for(int k = 0; k < K; k ++){
				theta1.add(new HashMap<String, Double>(N));
			}
			
			p1 = new double[K];
			
			normalize();
			
			
						
			//print(); // for debug
		}
			
		
	}
	

	
	private void set(){
		p0[0] = 1.0 / 3;
		p0[1] = 1.0 / 3;
		p0[2] = 1.0 / 3;
		
		
		Map<String, Double> dis0 = new TreeMap<String, Double>();
		dis0.put("A", 0.7);
		dis0.put("B", 0.3);
		dis0.put("C", 0.1);
					
		theta0.set(0, dis0);
		
		
		Map<String, Double> dis1 = new TreeMap<String, Double>();
		dis1.put("A", 0.5);
		dis1.put("B", 0.1);
		dis1.put("C", 0.4);
		
		theta0.set(1, dis1);
		
		
		Map<String, Double> dis2 = new TreeMap<String, Double>();
		dis2.put("A", 1.0 / 3);
		dis2.put("B", 1.0 / 3);
		dis2.put("C", 1.0 / 3);
		
		theta0.set(2, dis2);
		
	}
	
	private void normalize(){
		double s = 0.0;
		for(int k = 0; k < K; k ++){
			s += p0[k];
		}
		for(int k = 0; k < K; k ++){
			p0[k] /= s;
		}
		
		for(int k = 0; k < K; k ++){
			s = 0;
			Map<String, Double> theta = theta0.get(k);
			for(double value : theta.values()){
				s += value;
			}
			
			for(String word : theta.keySet()){
				double value = theta.get(word);
				theta.put(word, value / s);
			}
		}
	}
	
	
	private class Value{
		private double v = 0.0;
		public double get() { return v; }
		public void add(double a) { v += a; }
	}
	
	private double E(){
		final Value v = new Value();
		
		batch.scan(new UnitProcessor(){
			@Override
			public void processUnit(Map<String, Integer> unit) {
				// for pz
				double[] pz = new double[K];

				for (Map.Entry<String, Integer> entry : unit.entrySet()) {
					String word = entry.getKey();
					int count = entry.getValue();
					for (int k = 0; k < K; k++) {
						pz[k] += count * Math.log(theta0.get(k).get(word));
					}
				}

				for (int k = 0; k < K; k ++) {
					pz[k] = Math.exp(pz[k]);
					pz[k] = p0[k] * pz[k];
				}
				
				double s = 0.0;
				for (int k = 0; k < K; k ++){
					s += pz[k];
				}
				for (int k = 0; k < K; k ++){
					pz[k] /= s;
				}
				
				
				double e = 0.0;
				for (int k = 0; k < K; k ++){
					Map<String, Double> theta = theta0.get(k);
					double log = 0.0;
					log += Math.log(p0[k]);
					for(Map.Entry<String, Integer> entry : unit.entrySet()){
						String word = entry.getKey();
						int count = entry.getValue();
						log += count * Math.log(theta.get(word));
					}
					e += pz[k] * log;
				}
						
				v.add(e);
				
			}});
		
		return v.get();
	}
	
	public void processUnit(Map<String, Integer> unit){
		// for pz
		double[] pz = new double[K];

		for (Map.Entry<String, Integer> entry : unit.entrySet()) {
			String word = entry.getKey();
			int count = entry.getValue();
			for (int k = 0; k < K; k++) {
				pz[k] += count * Math.log(theta0.get(k).get(word));
			}
		}

		for (int k = 0; k < K; k++) {
			pz[k] = Math.exp(pz[k]);
			pz[k] = p0[k] * pz[k];
		}
		
		double s = 0.0;
		for (int k = 0; k < K; k ++){
			s += pz[k];
		}
		for (int k = 0; k < K; k ++){
			pz[k] /= s;
		}
		
		// for theta
		for(Map.Entry<String, Integer> entry : unit.entrySet()){
			String word = entry.getKey();
			int count = entry.getValue();
			for(int k = 0; k < K; k ++){
				Double v = theta1.get(k).get(word); // += count * pz[k];
				if(v == null){
					theta1.get(k).put(word, count * pz[k]);
				}else{
					theta1.get(k).put(word, v + count * pz[k]);
				}
			}
		}
		
		// for p
		for(int k = 0; k < K; k ++){
			p1[k] += pz[k];
		}

	}
	
	public void print(){
		System.out.println("p--------------\t");
		for(int k = 0; k < K; k ++){
			System.out.print(p0[k] + "\t");
		}
		System.out.println();
		
		System.out.println("theta--------------\t");
		for(int k = 0; k < K; k ++){
			Map<String, Double> theta = theta0.get(k);
			for(Map.Entry<String, Double> entry : theta.entrySet()){
				String word = entry.getKey();
				double value = entry.getValue();
				System.out.print(word + ":" + value + "\t");
			}
			System.out.println();
		}
		
	}
	
	public static void main(String[] args){
		Batch batch = new BatchSimulator(false);
		
		EM em = new EM();
		em.learn(batch, 3, 10, 100);
		
		em.print();
	}
	

}
