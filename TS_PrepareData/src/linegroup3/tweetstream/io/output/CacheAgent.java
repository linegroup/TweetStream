package linegroup3.tweetstream.io.output;

import linegroup3.tweetstream.event.OnlineEvent;



public class CacheAgent{
	
	//static private Cache cache = new RedisCache();
	
	static private Cache cache = new Cache(){
		private int id = 0;
		@Override
		public String getId() {
			id ++;
			return "" + id;
		}

		@Override
		public void put(String id, OnlineEvent event) {
			System.out.println("put:" + "\t" + event.toString());
		}

		@Override
		public void update(String id, OnlineEvent event) {
			System.out.println("update:" + "\t" + event.toString());			
		}
		
	};

	public static void set(Cache c){
		cache = c;
	}
	
	public static Cache get(){
		return cache;
	}
	
}
