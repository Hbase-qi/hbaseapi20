package com.qi.weibo.service;

import com.qi.weibo.dao.FocusDAO;

public class FocusService {

	private FocusDAO focusDAO;

	public void setFocusDAO() {
		this.focusDAO = new FocusDAO();
		focusDAO.setTable();
	}

	public void focusOperate(String currentUerId, String otherUserId) throws Exception {

		focusDAO.focusOperate(currentUerId, otherUserId);
		System.out.println("关注完成");
		
	}
	
	public void searchFocusUser(String userId) throws Exception {
		focusDAO.searchFocusUser(userId);
		System.out.println("查询我关注的人完成");
	}
	
	public void searchOtherUser(String userId) throws Exception {
		focusDAO.searchOtherUser(userId);
		System.out.println("查询关注我的人完成");
	}
	
	public static void main(String[] args) throws Exception {
		FocusService focusService=new FocusService();
		focusService.setFocusDAO();
		
//		focusService.focusOperate("1", "3");
		
//		focusService.searchFocusUser("1");
		
		focusService.searchOtherUser("3");
		
		
	}
}
