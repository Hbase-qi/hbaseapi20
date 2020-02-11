package com.qi.rowkey;

import java.text.SimpleDateFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.qi.HBaseUtil;

public class InputSampleData {

	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
	private Table fileInfo = HBaseUtil.getTable("bd20:file_info");

	// 定义一个方法把实例中的数据插入到hbase中
	// 表: create 'bd20:file_info','i'
	// 对应字段列名称：id:1,CreateTime:20120904,Name:中国好声音第1期,Category:综艺,UserID:1
	public void putSampleData(String id, String createTime, String name, String category, String userID)
			throws Exception {

		byte[] rowkey = RowKeyTest.getRowKey(Long.valueOf(userID), format.parse(createTime).getTime(),
				Long.valueOf(id));
		Put put = new Put(rowkey);
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("id"), Bytes.toBytes(id));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(name));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("category"), Bytes.toBytes(category));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("createTime"), Bytes.toBytes(createTime));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("userId"), Bytes.toBytes(userID));
		// 获取table
		fileInfo.put(put);
	}

	// 查询表中userid=1，时间范围为：20120901,20121001的数据(范围查询散列值必须一样)
	// 最小rowkey ：userid+20120901+long.min_value
	// 最大rowkey : userid+20121001+long.max_value
	// 关系型数据库中date字段与fileId字段是经常用来做查询限定的字段
	public void searchByUserDate(String userId, String startDate, String stopDate) throws Exception {
			//包含20120902时间值rowkwy中的fileId最大值小于 Long.MIN_VALUE,因此不包含此条记录,取值从20120903开始
		byte[] startRowKey = RowKeyTest.getRowKey(Long.valueOf(userId), format.parse(startDate).getTime(), Long.MIN_VALUE);
			//包含20120906时间值rowkwy中的fileId最小值大于于 0L,因此也包含此条记录,取值从20120905结束
		byte[] stopRowkey = RowKeyTest.getRowKey(Long.parseLong(userId), format.parse(stopDate).getTime(), 0L);

		Scan scan = new Scan();
		scan.setStartRow(startRowKey);//按位比较最小值
		scan.setStopRow(stopRowkey);//按位比较最大值
		ResultScanner resultScanner = fileInfo.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);

	}

	public static void main(String[] args) throws Exception {
		InputSampleData inputSampleData = new InputSampleData();
		 inputSampleData.putSampleData("1", "20120902", "中国好声音第1期", "综艺",
		 "1");
		 inputSampleData.putSampleData("2", "20120904", "中国好声音第2期", "综艺",
		 "1");
		 inputSampleData.putSampleData("3", "20120906", "中国好声音外卡赛", "综艺",
		 "1");
		 inputSampleData.putSampleData("4", "20120908", "中国好声音第3期", "综艺",
		 "1");
		 inputSampleData.putSampleData("5", "20120910", "中国好声音第4期", "综艺",
		 "1");
		 inputSampleData.putSampleData("6", "20120912", "中国好声音选手采访", "综艺花絮",
		 "2");
		 inputSampleData.putSampleData("7", "20120914", "中国好声音第5期", "综艺",
		 "1");
		 inputSampleData.putSampleData("8", "20120916", "中国好声音录制花絮", "综艺花絮",
		 "2");
		 inputSampleData.putSampleData("9", "20120918", "张玮独家专访", "花絮", "3");
		 inputSampleData.putSampleData("10", "20120920", "加多宝凉茶广告", "综艺广告",
		 "4");

//		inputSampleData.searchByUserDate("1", "20120902", "20120906");

	}

}
