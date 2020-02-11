package com.qi.weibo.service;


import java.util.Date;

import com.qi.weibo.UserEntity;
import com.qi.weibo.dao.UserDAO;

public class UserService {

	private UserDAO userDAO;
	
	public void setUserDao() {
		this.userDAO=new UserDAO();
		userDAO.setTable();
	}
	//注册账号 保存人员信息
	public void saveUserInfo(UserEntity user) throws Exception {
		userDAO.putUser(user);
	}
	//根据账号查询用户信息
	public UserEntity getUserByAccountNo(String accountNo) throws Exception {
		
		return userDAO.getUserByAccountNo(accountNo);
	}
	//测试service保存注册信息
	public static void main(String[] args) throws Exception {
		UserService service = new UserService();
		UserEntity userEntity = new UserEntity();
		userEntity.setAccountNo("123abc");
		userEntity.setAge(18);
		userEntity.setGender("女");
		userEntity.setHeadImage("http://dfdfdas/ff.jpg");
		userEntity.setName("Grace");
		userEntity.setPassword("123456");
		userEntity.setPhoneNo("13524643221");
		userEntity.setTimestamp(String.valueOf(new Date().getTime()));
		
		service.setUserDao();
		service.saveUserInfo(userEntity);
		UserEntity result = service.getUserByAccountNo("123abc");
		System.out.println(result);
	}
}
