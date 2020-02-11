package com.qi.weibo.dao;

import java.util.Arrays;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.qi.HBaseUtil;
import com.qi.weibo.WeiBoUtils;

public class FocusDAO {

	private Table table;

	public void setTable() {
		this.table = HBaseUtil.getTable("bd20:w_user");
	}

	public void focusOperate(String currentUerId, String otherUserId) throws Exception {

		byte[] currentUerRowkey = WeiBoUtils.getRowKeyByUserId(currentUerId,table);
		
		Get get=new Get(currentUerRowkey);
	
		Result result=table.get(get);
		HBaseUtil.printResult(result);
		
		Put currentPut=new Put(currentUerRowkey);
		currentPut.addColumn(Bytes.toBytes("c"), Bytes.toBytes("f"+otherUserId), Bytes.toBytes("0"));
		table.put(currentPut);
		
		byte[] otherUerRowkey = WeiBoUtils.getRowKeyByUserId(otherUserId,table);
		Put otherPut=new Put(otherUerRowkey);
		otherPut.addColumn(Bytes.toBytes("c"), Bytes.toBytes("u"+currentUerId), Bytes.toBytes("0"));
		table.put(otherPut);
	}
	
	public void searchFocusUser(String userId) throws Exception {
		
		Filter rowFilter=new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(userId)));
		Filter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("c")));
		Filter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("f")));
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

		filterList.addFilter(rowFilter);
		filterList.addFilter(familyFilter);
		filterList.addFilter(qualifierFilter);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = table.getScanner(scan);

		HBaseUtil.printResultScanner(resultScanner);
	}
	
public void searchOtherUser(String userId) throws Exception {
		
		Filter rowFilter=new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(userId)));
		Filter familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("c")));
		Filter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("u")));
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

		filterList.addFilter(rowFilter);
		filterList.addFilter(familyFilter);
		filterList.addFilter(qualifierFilter);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = table.getScanner(scan);

		HBaseUtil.printResultScanner(resultScanner);
	}

	
}
