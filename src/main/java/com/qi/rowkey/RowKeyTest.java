package com.qi.rowkey;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyTest {

	//生成rowkey
	//userId+createTime+fileId
	//6+8+8
	//rowkey长度是22字节
	public static byte[] getRowKey(long userId,long createTime,long fileId){
		
		//定义一个22字节的bytebuffer
		ByteBuffer buffer=ByteBuffer.allocate(22);
		//定义一个容纳6字节的bytebuffer来容纳userId
		ByteBuffer userIdBuffer=ByteBuffer.allocate(6);
		//为了散列userId需要逆向
		String userIdStrRev=StringUtils.reverse(String.valueOf(userId));
		userIdBuffer.put(Bytes.toBytes(userIdStrRev));
		//如果userIdBuffer没有填满，则在后面补"0"
		while (userIdBuffer.hasRemaining()) {
			userIdBuffer.put(Bytes.toBytes("0"));
		}
		//检验userIdBuffer是否正确
		System.out.println(Bytes.toString(userIdBuffer.array()));
		byte[] ctime=Bytes.toBytes(createTime);
		System.out.println("createTime长度:"+ctime.length);
		//把三部分按照顺序组装到buffer中
		buffer.put(userIdBuffer.array());
		buffer.put(Bytes.toBytes(createTime));
		buffer.put(Bytes.toBytes(fileId));
//		System.out.println(Arrays.toString(buffer.array()));
		return buffer.array();
	}
	
	public static void main(String[] args) {
		getRowKey(123, 2, 3);
	}
}
