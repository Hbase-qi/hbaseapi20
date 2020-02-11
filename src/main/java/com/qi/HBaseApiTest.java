package com.qi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseApiTest {

	private Configuration configuration=HBaseConfiguration.create();
	private Connection connection;
	private Admin admin;
	
	public HBaseApiTest() throws IOException{
		connection=ConnectionFactory.createConnection(configuration);
		admin=connection.getAdmin();
	} 
	
	public void createTable(String tName,String... colFamily) {
		
		HTableDescriptor desc=new HTableDescriptor(TableName.valueOf(tName));
		for (String cf : colFamily) {
			desc.addFamily(new HColumnDescriptor(cf));
		}
		try {
			admin.createTable(desc);
			System.out.println("表创建成功");
		} catch (IOException e) {
			System.err.println("表创建失败");
			e.printStackTrace();
		}
	}
	
	public void close() {
		if (connection!=null) {
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		HBaseApiTest test=new HBaseApiTest();
		test.createTable("bd20:w_user", "i","c");
		test.close();
	}
}
