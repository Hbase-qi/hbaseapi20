package com.qi.weibo;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.qi.HBaseUtil;

public class WeiBoUtils {

	public static final byte BLANK_STR = 0x1F;
	public static final byte ZERO = 0x00;
	public static final byte ONE = 0x01;

	public static byte[] getUserRowKey(String accountNo, String phoneNo) throws Exception {
		// 1.获取唯一标识
		// 调用getSequenceByTableName,传"w_user"为参数,获取w_user的递增序列
		long sequence = getSequenceByTableName("bd20:w_user");
		// 把获取到的值,转成字符串,逆向
		String pk = StringUtils.reverse(String.valueOf(sequence));
		// 然后根据长度在后面补0
		int coverNum = 12 - pk.length();
		pk = converLeft(pk, '0', coverNum);
		// 2.对accountNo限定长度或补长0x1f
		byte[] account = coverLeft(accountNo, 17);
		// 3.对phoneNo限定长度或者补长 0x1f
		byte[] phone = coverLeft(phoneNo, 11);
		// 4.组装上面三个获取到rowkey
		ByteBuffer buffer = ByteBuffer.allocate(40);
		buffer.put(Bytes.toBytes(pk));
		buffer.put(account);
		buffer.put(phone);
		return buffer.array();
	}

	// 从sequence表中获取递增序列
	public static long getSequenceByTableName(String tableName) throws Exception {
		// 对bd20:sequence表调用incr指令，rowkey是tableName，获取到的数据即递增的sequence，转成long返回
		Table sequence = HBaseUtil.getTable("bd20:squence");
		long result = sequence.incrementColumnValue(Bytes.toBytes(tableName), Bytes.toBytes("i"), Bytes.toBytes("s"),
				1);

		return result;
	}

	// 把给定的字符串补另一个字符串，补N次
	public static String converLeft(String source, char cover, int times) {

		while (times > 0) {
			source += cover;
			times -= 1;
		}
		return source;
	}

	// 给定字符串，转换成字节数组后，空白的在左边补0x1f
	public static byte[] coverLeft(String source, int length) {
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.put(Bytes.toBytes(source));
		while (buffer.hasRemaining()) {
			buffer.put(BLANK_STR);
		}
		return buffer.array();
	}

	// 给ByteBuffer往后面补N个字节
	public static void coverLeft(ByteBuffer source, int length, byte cover) {
		while (length > 0) {
			source.put(cover);
			length -= 1;
		}
	}
	
	public static byte[] getRowKeyByUserId(String userId,Table table) throws IOException {
		
//		String pk = StringUtils.reverse(String.valueOf(userId));
//		// 然后根据长度在后面补0
//		int coverNum = 12 - pk.length();
//		pk = converLeft(pk, '0', coverNum);
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(Bytes.toBytes(userId));
		Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		Result result = resultScanner.next();
		byte[] rowkey = result.getRow();
		return rowkey;
	}

	public static void main(String[] args) throws Exception {
		// long result=getSequenceByTableName("w_user");
		// System.out.println(result);
		// String result=converLeft("123", '0', 5);
		// System.out.println(result);

		// byte[] userRowKey = getUserRowKey("qwert", "987654321");
		// System.out.println(userRowKey.length);
		// System.out.println(Bytes.toString(userRowKey));

		ByteBuffer test = ByteBuffer.allocate(20);
		test.put(Bytes.toBytes("abc"));
		byte cover = 0x1f;
		coverLeft(test, 17, cover);
		System.out.println(test.array().length);
		System.out.println(Bytes.toString(test.array()));

	}
}
