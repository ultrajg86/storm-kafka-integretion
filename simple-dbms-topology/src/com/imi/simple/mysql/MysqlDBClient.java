package com.imi.simple.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import com.google.common.base.Joiner;

public class MysqlDBClient {
	
	private Statement st;
	
	public MysqlDBClient(String url, String user, String password){
		try{
			Connection con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
		}catch(Exception ex){
			ex.printStackTrace();
		}		
	}
	
	public void insert(String table, Object[] values){
		try{
			
			String query = "INSERT INTO " + table + " SET ";
			query += Joiner.on(",").join(values);
			System.out.println(query);
			//ResultSet rs = this.st.executeQuery(query);
			this.st.executeUpdate(query);
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	

}
