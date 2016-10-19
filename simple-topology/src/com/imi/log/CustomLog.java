package com.imi.log;

import java.io.FileWriter;
import java.io.IOException;

public class CustomLog {

	String logFile = "simple-topology.log";
	FileWriter fw;
	static final String ENTER = System.getProperty("line.separator");

	public CustomLog(){
		try{
			fw = new FileWriter(logFile, true);
		}catch(IOException e){}
	}
	
	public void close(){
		try{
			fw.close();
		}catch(IOException e){}
	}
	
	public void info(String msg){
		try{
			fw.write(msg + ENTER);
			fw.flush();
		}catch(IOException e){}
	}
}
