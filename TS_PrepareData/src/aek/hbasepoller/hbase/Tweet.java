package aek.hbasepoller.hbase;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A modified version of Philips' Tweet class.
 * Add additional publishedTimeGmtStr field 
 * @author Aek
 *
 */
public class Tweet {
	private long statusId = 0;
	private long userId = 0;
	private long publishedTimeGmt = 0;
	private String publishedTimeGmtStr = null;
	private String content = null;
	private String geo = null;
	
	public Tweet(long statusId, long userId, long publishedTimeGmt,
			String content, String geo) {
		super();
		this.statusId = statusId;
		this.userId = userId;
		this.publishedTimeGmt = publishedTimeGmt;
		DateFormat utcDf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcDf.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date d = new Date();
		d.setTime(this.publishedTimeGmt);
		this.publishedTimeGmtStr = utcDf.format(d);
		this.geo = geo;
		try {
			this.content = new String(content.getBytes("UTF-8"),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}
	
	public long getStatusId() {
		return statusId;
	}

	public long getUserId() {
		return userId;
	}

	public long getPublishedTimeGmt() {
		return publishedTimeGmt;
	}
	
	public String getPublishedTimeGmtStr() {
		return publishedTimeGmtStr;
	}

	public String getContent() {
		return content;
	}
	
	public String getGeo(){
		return geo;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((content == null) ? 0 : content.hashCode());
		result = prime * result
				+ (int) (publishedTimeGmt ^ (publishedTimeGmt >>> 32));
		result = prime * result + (int) (statusId ^ (statusId >>> 32));
		result = prime * result + (int) (userId ^ (userId >>> 32));
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		Tweet other = (Tweet) obj;
		if (content == null) {
			if (other.content != null) return false;
		} 
		else if (!content.equals(other.content)) return false;
		if (geo != other.geo) return false;
		if (publishedTimeGmt != other.publishedTimeGmt) return false;
		if (statusId != other.statusId) return false;
		if (userId != other.userId) return false;
		return true;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("statusId: " + statusId);
		sb.append("\nuserId: " + userId);
		sb.append("\npublishedTimeGmt: " + publishedTimeGmt);
		sb.append("\npublishedTimeGmtStr: " + publishedTimeGmtStr);
		sb.append("\ncontent: " + content);
		sb.append("\ngeo: " + geo);
		return sb.toString();
	}
}