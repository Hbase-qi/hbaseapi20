package com.qi.weibo.dao;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.qi.HBaseUtil;
import com.qi.weibo.UserEntity;
import com.qi.weibo.WeiBoUtils;

public class UserDAO {

	private Table table;
	
	public void setTable() {
		this.table=HBaseUtil.getTable("bd20:w_user");
	}
	
	public void putUser(UserEntity userEntity) throws Exception {
		//判断是否新增即userEntity的rowkey是否为空个，若为空则生成并set进来
		if (userEntity.getRowkey()==null) {
			byte[] rowkey = WeiBoUtils.getUserRowKey(userEntity.getAccountNo(), userEntity.getPhoneNo());
			userEntity.setRowkey(rowkey);
		}
		
		Put put=new Put(userEntity.getRowkey());
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("name"), Bytes.toBytes(userEntity.getName()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("gender"), Bytes.toBytes(userEntity.getGender()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("age"), Bytes.toBytes(userEntity.getAge()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("phoneNo"), Bytes.toBytes(userEntity.getPhoneNo()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("accountNo"), Bytes.toBytes(userEntity.getAccountNo()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("password"), Bytes.toBytes(userEntity.getPassword()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("headImage"), Bytes.toBytes(userEntity.getHeadImage()));
		put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("timestamp"), Bytes.toBytes(userEntity.getTimestamp()));
		
		table.put(put);
	}
	
	public UserEntity getUserByAccountNo(String accountNo) throws Exception {
		ByteBuffer account=ByteBuffer.allocate(17);
		account.put(Bytes.toBytes(accountNo));
		while (account.hasRemaining()) {
			account.put(WeiBoUtils.BLANK_STR);
		}
		ByteBuffer compareRowKey=ByteBuffer.allocate(40);
		
		WeiBoUtils.coverLeft(compareRowKey, 12, WeiBoUtils.ZERO);
		compareRowKey.put(account.array());
		WeiBoUtils.coverLeft(compareRowKey, 11, WeiBoUtils.ZERO);
		
		ByteBuffer compareInfo=ByteBuffer.allocate(40);
		WeiBoUtils.coverLeft(compareInfo, 12,WeiBoUtils.ONE);
		WeiBoUtils.coverLeft(compareInfo, 17,WeiBoUtils.ZERO);
		WeiBoUtils.coverLeft(compareInfo, 11,WeiBoUtils.ONE);
		
		Pair<byte[], byte[]> fuzzyParam=new Pair<>();
		fuzzyParam.setFirst(compareRowKey.array());
		fuzzyParam.setSecond(compareInfo.array());
		
		List<Pair<byte[], byte[]>> fuzzyParamList=new ArrayList<>();
		fuzzyParamList.add(fuzzyParam);
		
		Filter filter=new FuzzyRowFilter(fuzzyParamList);
		Scan scan=new Scan();
		
		scan.setFilter(filter);
		
		ResultScanner resultScanner = table.getScanner(scan);
		
		Result result = resultScanner.next();
		if(result != null){
			UserEntity userEntity = new UserEntity();
			userEntity.setRowkey(result.getRow());
			userEntity.setAccountNo(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("accountNo"))));
			userEntity.setAge(Bytes.toInt(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("age"))));
			userEntity.setGender(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("gender"))));
			userEntity.setHeadImage(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("headImage"))));
			userEntity.setName(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("name"))));
			userEntity.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("password"))));
			userEntity.setPhoneNo(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("phoneNo"))));
			userEntity.setTimestamp(Bytes.toString(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("timestamp"))));
			return userEntity;
		}else{
			return null;
		}
	}
	
}
