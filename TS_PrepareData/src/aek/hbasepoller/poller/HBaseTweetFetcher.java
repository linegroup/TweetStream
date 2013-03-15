package aek.hbasepoller.poller;
import aek.hbasepoller.hbase.PartialRowKey;
import aek.hbasepoller.hbase.RowKeys;
import aek.hbasepoller.hbase.Tweet;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.gotometrics.orderly.StructRowKey;
import com.sematext.hbase.wd.DistributedScanner;

/**
 * Fetch current tweets from a specified HBase table
 * @author aek
 *
 */
public class HBaseTweetFetcher {
	
	private Configuration conf = null;
	private HTablePool pool = null;
	private DateFormat utcDf = null;
	private String resourceName = null;
	private int lagMinute = 0;
	private int intervalMinute = 0;
	private List<Tweet> curFetch = null;
	
	public HBaseTweetFetcher(String resourceName, int lagMinute, int intervalMinute) throws IOException {
		if(resourceName == null || resourceName.length() == 0) throw new IOException("Invalid table name: " + resourceName);
		if(intervalMinute < 0) throw new IOException("Interval cannot be negative.");
		utcDf = new SimpleDateFormat(Config.getParameter("hbase_tweet_date_format"));
		utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));

		// Specify HBase configuration file
		conf = HBaseConfiguration.create();
		conf.addResource(new Path(Config.getParameter("hbase_client_config")));

		// Create a new connection pool
		pool = new HTablePool(conf, Integer.parseInt(Config.getParameter("hbase_max_connections")));
		
		this.resourceName = resourceName;
		this.lagMinute = lagMinute;
		this.intervalMinute = intervalMinute;
	}
	
	public void fetch() throws IOException{
		this.curFetch = Lists.newArrayList();
		Date end = new Date(new Date().getTime()  - (this.lagMinute*60*1000));
		Date start = new Date(end.getTime() - (this.intervalMinute*60*1000));
		HTableInterface table = pool.getTable(this.resourceName);
		
		// Orderly and HBaseWD stuff for generating fancy row keys.
		StructRowKey rowKey = RowKeys.buildStructRowKey(RowKeys.tweetRowKey());
		StructRowKey filterKey = RowKeys.buildStructRowKey(PartialRowKey.tweetPublishedTimeGmt());
		byte[] prefixStartRK = filterKey.serialize(new Object[]{Long.MAX_VALUE - end.getTime()});
		byte[] prefixEndRK = filterKey.serialize(new Object[]{Long.MAX_VALUE - start.getTime()});
		
		// Create a new Scan and ResultScanner and prepare to retrieve the data across nodes
		Scan scan = new Scan(prefixStartRK, prefixEndRK);
		scan.setBatch(Integer.parseInt(Config.getParameter("hbase_scan_batch_size")));
		scan.setCaching(Integer.parseInt(Config.getParameter("hbase_scan_batch_size")));
		ResultScanner rs = DistributedScanner.create((HTable) table, scan, RowKeys.keyDistributor);
		byte[] oKey = null;
		Object[] keys = null;
		Long publishedTimeGMT = null;
		Long userId = null;
		Long statusId = null;
		String content = null;
		String geo = null;
		long retweetCount = 0;
		
		for (Result cur: rs) {
			oKey = RowKeys.keyDistributor.getOriginalKey(cur.getRow());			
			keys = (Object[]) rowKey.deserialize(oKey);
			if(keys == null || keys.length !=4){
				throw new IOException("Invalid key: " + keys);
			}
			publishedTimeGMT = Long.MAX_VALUE - (Long) keys[0];
			userId = (Long) keys[1];
			statusId = (Long) keys[2];
			content = Bytes.toString(cur.getValue(Bytes.toBytes("content"), Bytes.toBytes("content")));
			geo = Bytes.toString(cur.getValue(Bytes.toBytes("location"), Bytes.toBytes("geo"))); 
			this.curFetch.add(new Tweet(statusId, userId, publishedTimeGMT, content, geo));
		}
		rs.close();
		table.close();
	}
	
	public List<Tweet> getCurFetch(){
		return this.curFetch;
	}
	
	public String getCurFetchJSONString(){
		return new Gson().toJson(this.curFetch);
	}

	public String getHTableName(){
		return this.resourceName;
	}
	
	public int getLagMinute(){
		return this.lagMinute;
	}
	
	public int getIntervalMinute(){
		return this.intervalMinute;
	}
}