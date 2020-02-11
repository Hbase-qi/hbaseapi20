package com.qi.hbasePage;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.qi.HBaseUtil;

//hbase上如何实现：
//1.PageFilter：看一下就行了
//2.获取到ResultScanner
 // 然后空掉ResultScanner的next(int arg0)方法,掉用next((pageIndex-1)*pageSize)，不取数据
  //然后从下一个next(pageSize)处开始取数据，然后取pageSize条


public class HBasePage {
	
	private static Table table = HBaseUtil.getTable("bd20:file_info");
	
	public static void getInfoByPage(int pageIndex,int pageSize) throws Exception {
		Scan scan=new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		resultScanner.next((pageIndex-1)*pageSize);
		Result[] results2 = resultScanner.next(pageSize);
		for (Result result : results2) {
			HBaseUtil.printResult(result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		getInfoByPage(2, 3);
	}
}
