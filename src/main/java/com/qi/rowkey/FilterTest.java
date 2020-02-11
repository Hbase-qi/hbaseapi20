package com.qi.rowkey;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.qi.HBaseUtil;

public class FilterTest {

	private Table table = HBaseUtil.getTable("bd20:file_info");
	// private Table table = HBaseUtil.getTable("bd20:person_info");
	private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

	// 查询创建时间createtime=20120904的所有数据
	// 使用rowkey的filter(RowFilter)来进行比较，需要判断rowkey的数据内部要包含20120904
	// 比较器：SubstringComparator
	// 操作符EQUAL
	public void searchByCreateTime(String createTime) throws Exception {

		Scan scan = new Scan();
		long ctime = format.parse(createTime).getTime();
		byte[] ltobytes = Bytes.toBytes(ctime);
		// 创建比较器
		SubstringComparator comparator = new SubstringComparator(Bytes.toString(ltobytes));
		// 创建过滤器，指定比较器和操作符
		Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		// 把比较器添加到scan上
		scan.setFilter(filter);
		// 使用table来scan
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);

	}
	// 使用rowfilter查询出userId=3 并且createtime=20120918的数据内容
	// rowkey：UserID + CreateTime + FileID
	// 过滤器：RowFilter
	// 比较器：BinaryPrefixComparator
	// 运算符：CompareOp.EQUAL

	public void searchByUserIdCreateTime(String userId, String createTime) throws Exception {
		ByteBuffer prefix = ByteBuffer.allocate(14);
		ByteBuffer userBuffer = ByteBuffer.allocate(6);
		userBuffer.put(Bytes.toBytes(StringUtils.reverse(userId)));
		while (userBuffer.hasRemaining()) {
			userBuffer.put(Bytes.toBytes("0"));
		}
		long ctime = format.parse(createTime).getTime();
		prefix.put(userBuffer.array());
		prefix.put(Bytes.toBytes(ctime));
		// 创建比较器
		BinaryPrefixComparator comparator = new BinaryPrefixComparator(prefix.array());
		// 创建过滤器
		Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		// 把过滤器设置到scan上
		Scan scan = new Scan();
		scan.setFilter(filter);
		// 调用table的scan方法，使用scan对象来作为扫描配置
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}
	// 查询hbase的bd20:file_info表中，列簇为i的所有数据
	// 过滤器：FamilyFilter
	// 比较器: BinaryComparator
	// 运算符：CompareOp.EQUAL

	// 人员信息表
	// 人员的基本信息、人员联系方式信息
	// 基本信息：姓名、性别、年龄、身份证号 。。。
	// 联系方式：电话
	// 邮寄地址
	// create 'bd20:person_info','i','c'
	// put 'bd20:person_info','1','i:name','abc'
	// put 'bd20:person_info','1','i:age','18'
	// put 'bd20:person_info','1','c:phone_1','19283827262'
	// put 'bd20:person_info','1','c:phone_2','34222232333'
	// put 'bd20:person_info','1','c:mail_1','34222232333'
	// put 'bd20:person_info','1','c:mail_2','34222232333'
	// person_info
	// i c
	// name phone_1
	// gender phone_2
	// age mail_1
	// id_card mail_2
	//
	// 查询出person_info里面所有的电话号
	// c列簇下的qialify like 'phone_%'
	// 过滤器：QualifyFilter
	// 比较器：RegexStringComparator（SubstringComparator，BinaryPrefixComparator）
	// 运算符：CompareOp.EQUAL

	public void getPhoneFromPerson() throws Exception {

		RegexStringComparator comparator = new RegexStringComparator("^phone_\\d+");
		Filter filter = new QualifierFilter(CompareOp.EQUAL, comparator);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);

	}

	// 使用专有过滤器来实现查询出person_info里面所有的电话号需求
	public void getPhonesFromPerson2() throws Exception {
		// 定义专有过滤器
		ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("phone_"));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}

	public void getfamilyInfoFromFileInfo() throws Exception {

		BinaryComparator comparator = new BinaryComparator(Bytes.toBytes("i"));
		Filter filter = new FamilyFilter(CompareOp.EQUAL, comparator);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);

	}

	// 查询列值包含“中国好声音的”数据
	// 过滤器：ValueFilter
	// 比较器：BinaryComparator
	// 操作符：CompareOp.EQUAL

	public void getInfoFromFileInfoByValue() throws Exception {

		SubstringComparator comparator = new SubstringComparator("中国好声音");
		Filter filter = new ValueFilter(CompareOp.EQUAL, comparator);
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);
	}

	// 查询文件名称包含“中国好声音的”数据
	// 1：列簇为：i
	// 2: 列名称: name
	// 3: 列值：包含“中国好声音的”
	// 改需求其实需要3个filter来完成对扫描的限定的
	// 3个filter是and的关系
	public void getDataByFileNameRight() throws Exception {
		// 定义第一个过滤器：FamilyFilter,BinaryComparator,CompareOp.EQUAL
		Filter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("i")));
		// 定义第二个过滤器: QualifierFilter,BinaryComparator,CompareOp.EQUAL
		Filter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
		// 定义第三个过滤器: ValueFilter,SubstringComparator,CompareOp.EQUAL
		Filter valueFilter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("中国好声音"));
		// 定义FilterList过滤器，把前三个过滤器加到FilterList里面，并且三个过滤器的关系是“and”
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

		filterList.addFilter(familyFilter);
		filterList.addFilter(qualifierFilter);
		filterList.addFilter(valueFilter);

		Scan scan = new Scan();
		scan.setFilter(filterList);
		// 用table来配置scan对数据扫描，并获取到数据结果
		ResultScanner resultScanner = table.getScanner(scan);

		HBaseUtil.printResultScanner(resultScanner);
	}

	// 查询： 20120910~20120914的数据文件
	// 查询： 20120910的数据文件信息
	public void fuzzyFilterTest() throws Exception {
		// 参数1：被比较的值
		// 【x】【x】【x】【x】【x】【x】 【2】【0】【1】【2】【0】【9】【1】【0】 【x】【x】【x】【x】【x】【x】【x】【x】
		ByteBuffer compareRowKey = ByteBuffer.allocate(22);
		for (int i = 0; i < 6; i++) {
			compareRowKey.put(Bytes.toBytes("0"));
		}
		long createTime = format.parse("20120910").getTime();
		compareRowKey.put(Bytes.toBytes(createTime));
		while (compareRowKey.hasRemaining()) {
			compareRowKey.put(Bytes.toBytes("0"));
		}

		// 参数2：比较位数设定(只比较第7个字节到14个字节（共8个字节）的内容)
		// 【1】【1】【1】【1】【1】【1】 【0】【0】【0】【0】【0】【0】【0】【0】 【1】【1】【1】【1】【1】【1】【1】【1】
		ByteBuffer compareInfo = ByteBuffer.allocate(22);
		byte zero = 0x00;
		byte one = 0x01;

		for (int i = 0; i < 6; i++) {
			compareInfo.put(one);
		}
		for (int i = 0; i < 8; i++) {
			compareInfo.put(zero);
		}
		while (compareInfo.hasRemaining()) {
			compareInfo.put(one);
		}

		Pair<byte[], byte[]> pair = new Pair<>();
		pair.setFirst(compareRowKey.array());
		pair.setSecond(compareInfo.array());

		List<Pair<byte[], byte[]>> param = new ArrayList<>();
		//	可构建多个值实现范围查找如:查找20120910~20120914的数据文件
		param.add(pair);
		//使用param构建 fuzzyFilter
		FuzzyRowFilter filter = new FuzzyRowFilter(param);

		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		HBaseUtil.printResultScanner(resultScanner);

	}

	public static void main(String[] args) throws Exception {
		FilterTest test = new FilterTest();
		 test.searchByCreateTime("20120918");
		// test.searchByUserIdCreateTime("3", "20120918");
		// test.getPhoneFromPerson();
		// test.getfamilyInfoFromFileInfo();

		// test.getInfoFromFileInfoByValue();
		// test.getPhonesFromPerson2();

		// test.getDataByFileNameRight();

//		test.fuzzyFilterTest();

	}

}
