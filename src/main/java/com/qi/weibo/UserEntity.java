package com.qi.weibo;

public class UserEntity {
	private byte[] rowkey;
	private String name;
	private String gender;
	private int age;
	private String phoneNo;
	private String accountNo;
	private String password;
	private String headImage;
	private String timestamp;
	
	

	public byte[] getRowkey() {
		return rowkey;
	}
	public void setRowkey(byte[] rowkey) {
		this.rowkey = rowkey;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getPhoneNo() {
		return phoneNo;
	}
	public void setPhoneNo(String phoneNo) {
		this.phoneNo = phoneNo;
	}
	public String getAccountNo() {
		return accountNo;
	}
	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getHeadImage() {
		return headImage;
	}
	public void setHeadImage(String headImage) {
		this.headImage = headImage;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	@Override
	public String toString() {
		return "UserEntity [name=" + name + ", gender=" + gender + ", age=" + age + ", phoneNo=" + phoneNo
				+ ", accountNo=" + accountNo + ", password=" + password + ", headImage=" + headImage + ", timestamp="
				+ timestamp + "]";
	}
}
