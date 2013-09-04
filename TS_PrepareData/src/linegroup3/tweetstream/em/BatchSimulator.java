package linegroup3.tweetstream.em;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class BatchSimulator implements Batch{ // three topics 0.5, 0.3, 0.2
	Random rand = new Random();
	List<Map<String, Integer>> documents = null;
	
	public BatchSimulator(){
		documents = new LinkedList<Map<String, Integer>>();
		
		
		Map<String, Integer> unit = new TreeMap<String, Integer>();	
		unit.put("A", 10);
		unit.put("B", 20);
		unit.put("C", 30);
		documents.add(unit);
		
		unit = new TreeMap<String, Integer>();	
		unit.put("A", 20);
		unit.put("B", 10);
		unit.put("C", 10);
		documents.add(unit);
		
		unit = new TreeMap<String, Integer>();	
		unit.put("A", 20);
		unit.put("B", 30);
		unit.put("C", 10);
		documents.add(unit);
	}
	
	public BatchSimulator(boolean random){
		documents = new LinkedList<Map<String, Integer>>();
		for(int i = 0; i < 100; i ++){

			Map<String, Integer> unit = new TreeMap<String, Integer>();
			if(i < 50){
				unit.put("A", 10 + (random ? (rand.nextInt(3) - 1) : 0));
				unit.put("B", 20 + (random ? (rand.nextInt(3) - 1) : 0));
				unit.put("C", 30 + (random ? (rand.nextInt(3) - 1) : 0));
				documents.add(unit);
			}else{
				if(i < 80){
					unit.put("A", 20 + (random ? (rand.nextInt(3) - 1) : 0));
					unit.put("B", 10 + (random ? (rand.nextInt(3) - 1) : 0));
					unit.put("C", 10 + (random ? (rand.nextInt(3) - 1) : 0));
					documents.add(unit);					
				}else{
					unit.put("A", 20 + (random ? (rand.nextInt(3) - 1) : 0));
					unit.put("B", 30 + (random ? (rand.nextInt(3) - 1) : 0));
					unit.put("C", 10 + (random ? (rand.nextInt(3) - 1) : 0));
					documents.add(unit);					
				}
			}
		}
	}
	
	public void scan(UnitProcessor processor){
		for(Map<String, Integer> unit : documents){
			processor.processUnit(unit);
		}
	}

}
