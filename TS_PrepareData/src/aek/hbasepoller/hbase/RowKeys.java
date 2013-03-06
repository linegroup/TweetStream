package aek.hbasepoller.hbase;

import aek.hbasepoller.poller.Config;

import com.gotometrics.orderly.FixedUnsignedLongRowKey;
import com.gotometrics.orderly.IntegerRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.UTF8RowKey;
import com.gotometrics.orderly.UnsignedLongRowKey;
import com.sematext.hbase.wd.AbstractRowKeyDistributor;
import com.sematext.hbase.wd.RowKeyDistributorByHashPrefix;

/**
 * Philips' RowKeys helper class
 * @author palakorna
 *
 */
public class RowKeys {	
	public static final AbstractRowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(
			new RowKeyDistributorByHashPrefix.OneByteSimpleHash(Integer.parseInt(Config.getParameter("hbase_regionserver_count"))));
	
	public static StructRowKey buildStructRowKey(RowKey[] rk) {
		StructRowKey rowKey = new StructRowKey(rk);
		rowKey.setMustTerminate(true);
		return rowKey;
	}
	
	public static RowKey[] twitterersRowKey() {
		return new RowKey[] {
				new UnsignedLongRowKey(), // user_id
				new UTF8RowKey() // screen_name
		};
	}

	public static RowKey[] tweetRowKey() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey(), // published_time_gmt
				new UnsignedLongRowKey(), // user_id
				new UnsignedLongRowKey(), // status_id
				new IntegerRowKey() // is_deleted
		};
	}
	
	public static RowKey[] tweetUserIdxRowKey() {
		return new RowKey[] {
				new UnsignedLongRowKey(), // user_id
				new FixedUnsignedLongRowKey(), // published_time_gmt
				new UnsignedLongRowKey(), // status_id
				new IntegerRowKey() // is_deleted
		};
	}
	
	public static RowKey[] countRowKey() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey(),  // date + hour (minute and seconds are truncated)
				new UTF8RowKey() // token
		};
	}
	
	public static RowKey[] embedInfoRowKey() {
		return new RowKey[] {
				new UTF8RowKey()  // URL
		};
	}
	
	public static RowKey[] tweetPropertyRowKey() {
		return new RowKey[] {
				new UnsignedLongRowKey() // status_id
		};
	}
	
	public static RowKey[] mentionRowKey() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey(),  // published_time_gmt
				new UnsignedLongRowKey(), // mentioned user_id
				new UnsignedLongRowKey(), // status_id
				new UnsignedLongRowKey()  // mentioned by user_id
		};
	}
	
	public static RowKey[] urlsRowKey() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey(),  // published_time_gmt
				new UTF8RowKey(), // url
				new UnsignedLongRowKey(), // status_id
				new UTF8RowKey() // url type
		};
	}
	
	public static RowKey[] statusIdRowKey() {
		return new RowKey[] {
				new UnsignedLongRowKey() // status_id
		};
	}
	
	public static RowKey[] keywordBlacklistRowKey() {
		return new RowKey[] {
				new UTF8RowKey()  // keyword
		};
	}
	
	public static RowKey[] keywordTracklistRowKey() {
		return new RowKey[] {
				new UTF8RowKey()  // keyword
		};
	}
	
	public static RowKey[] landmarkRowKey() {
		return new RowKey[] {
				new UTF8RowKey()  // name
		};
	}	
}
