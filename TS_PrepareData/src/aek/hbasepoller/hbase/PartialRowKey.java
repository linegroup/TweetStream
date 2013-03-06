package aek.hbasepoller.hbase;
import com.gotometrics.orderly.FixedUnsignedLongRowKey;
import com.gotometrics.orderly.RowKey;
import com.gotometrics.orderly.UTF8RowKey;
import com.gotometrics.orderly.UnsignedLongRowKey;

/**
 * Philips' PartialRowKey helper class
 * @author palakorna
 *
 */
public class PartialRowKey {
	public static RowKey[] twitterersUserId() {
		return new RowKey[] {
				new UnsignedLongRowKey() // user_id 
		};
	}
	
	public static RowKey[] tweetPublishedTimeGmt() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey() // published_time_gmt
		};
	}

	public static RowKey[] tweetUserIdPublishedTimeGmt() {
		return new RowKey[] {
				new UnsignedLongRowKey(), // user_id
				new FixedUnsignedLongRowKey() // published_time_gmt
		};
	}
	
	public static RowKey[] countDateHour() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey() // date + hour (minute and seconds are truncated)
		};
	}
	
	public static RowKey[] mentionPublishedTimeGmt() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey() // published_time_gmt
		};
	}
	
	public static RowKey[] mentionUserIdPublishedTimeGmt() {
		return new RowKey[] {
				new UnsignedLongRowKey(), // mentioned user_id
				new FixedUnsignedLongRowKey() // published_time_gmt
		};
	}
	
	public static RowKey[] urlsPublishedTimeGmt() {
		return new RowKey[] {
				new FixedUnsignedLongRowKey() // published_time_gmt
		};
	}
	
	public static RowKey[] urlsTypePublishedTimeGmt() {
		return new RowKey[] {
				new UTF8RowKey(), // url type
				new FixedUnsignedLongRowKey()  // published_time_gmt
		};
	}
}