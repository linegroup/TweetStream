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
			ret = hashBase.APHash("ko" + i) % 200;
		}break;
		case 2:{
			ret = hashBase.DJBHash("eq" + i) % 200;
		}break;
		case 3:{
			ret = hashBase.JSHash("mx" + i) % 200;
		}break;
		case 4:{
			ret = hashBase.RSHash("wa" + i) % 200;
		}break;
		}
		
		if(ret < 0)	ret += 200;
		return (int)ret;
	}
	
	
	/*////// NEED TEST MORE ??????????????????
	private static long[] b = { 105037    , 105449    , 105907   , 105019    , 105613 };
	private static long[] a = { 1093   , 2917   , 3911   , 2129 , 131 };
	private static long P = 1300133;
	public static int hash(int fun, int i) { // return 0~199 fun 0~4
		long ret = ((a[fun] + i*b[fun]) % P ) % 200 ;
		
		if(ret < 0)	ret += 200;
		return (int)ret;
		
	}
	*/
	
	

}
