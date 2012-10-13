package linegroup3.tweetstream.preparedata;

public class HashFamily {

	/*
	private static long[] seeds = { 31, 131, 1313, 13131, 131313 };

	public static int hash(int fun, int i) { // return 0~199 fun 0~4

		long ret = BKDRHash(seeds[fun], "" + i) % 200;
		return (int)ret;
	}

	private static long BKDRHash(long seed, String str) {
		
		long hash = 0;

		for (int i = 0; i < str.length(); i++) {
			hash = (hash * seed) + str.charAt(i);
		}

		return hash;
	}
	/* End Of BKDR Hash Function */
	
	
	private static GeneralHashFunctionLibrary hashBase = new GeneralHashFunctionLibrary();
	
	public static int hash(int fun, int i) { // return 0~199 fun 0~4
		long ret = 0;
		switch(fun){
		case 0:{
			ret = hashBase.BKDRHash("yg" + i) % 200;
		}break;
		case 1:{
			ret = hashBase.PJWHash("ko" + i) % 200;
		}break;
		case 2:{
			ret = hashBase.ELFHash("eq" + i) % 200;
		}break;
		case 3:{
			ret = hashBase.JSHash("mx" + i) % 200;
		}break;
		case 4:{
			ret = hashBase.SDBMHash("wx" + i) % 200;
		}break;
		}
		
		return (int)ret;
	}
	
	
	/*
	private static long[] seeds = { 91, 101, 13, 701, 11011 };
	private static long[] b = { 241, 9856, 28, 128535, 6732 };
	public static int hash(int fun, int i) { // return 0~199 fun 0~4
		long ret = (b[fun] + i*seeds[fun]) % 200 ;
		
		return (int)ret;
		
	}
	*/
	

}
