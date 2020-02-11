package com.qi.hbasePage;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.qi.HBaseUtil;

//hbase�����ʵ�֣�
//1.PageFilter����һ�¾�����
//2.��ȡ��ResultScanner
 // Ȼ��յ�ResultScanner��next(int arg0)����,����next((pageIndex-1)*pageSize)����ȡ����
  //Ȼ�����һ��next(pageSize)����ʼȡ���ݣ�Ȼ��ȡpageSize��


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
