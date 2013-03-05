package linegroup3.tweetstream.io.input;

import org.json.JSONObject;

public class Buffer { 
	
	public Buffer(int size){
		BUF_SIZE = size;
		
		buffer = new JSONObject[BUF_SIZE];
	}
	
	private int size(){
		if(start == -1)
			return 0;
		
		return end > start ? end - start : end - start + BUF_SIZE;
	}
	
	public synchronized JSONObject put(JSONObject object){
		JSONObject ret = null;
		
		int s = size();
		
		if(start == -1 || s == BUF_SIZE){
			if(s == BUF_SIZE){
				ret = buffer[start];
			}
			start = (start + 1) % BUF_SIZE;
		}
		
		buffer[end] = object;
		end = (end + 1) % BUF_SIZE;
		
		return ret;
	}
	
	public synchronized void scan(ProcessObject processor){
		if(size() == 0) return;
		
		int s = start;
		int e = end;
		if(e <= s){
			e += BUF_SIZE;
		}
		
		for(int i = s; i < e; i ++){
			processor.process(buffer[i % BUF_SIZE]);
		}
	}
	
	
	 private int BUF_SIZE = 10000;
	 
	 private JSONObject[] buffer = null; 
	 
	 private int start = -1;
	 
	 private int end = 0;

}
