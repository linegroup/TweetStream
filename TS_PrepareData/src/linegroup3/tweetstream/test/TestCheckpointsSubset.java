package linegroup3.tweetstream.test;

import java.util.TreeSet;

public class TestCheckpointsSubset {
	
	private static TreeSet<Integer> set(){
		System.out.println("in");
		TreeSet<Integer> ret = new TreeSet<Integer>();
		ret.add(0);
		ret.add(1);
		ret.add(2);
		return ret;
	}
	
	public static void test(){
		for(int i : set()){
			System.out.println(i);
		}
	}

}
