package com.michaels.integration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppProperties {
	
	private static final String FILENAME = "config.properties";
	private static AppProperties instance;
    private Properties prop;
    
    private static String serverURI = "http://mac.localhost:8000";
    private static String cndServerURI = "http://en-ca.mac.localhost:8000";
    private static String injestPath = "data_in/";
    private static String selectClassCodes = "4|5|6|7";
    private static String archivePath = "archive";
    private static String username = "mac+super@skedge.me";
    private static String pwd = "schedule";
    private static String token = "p3v0UCDUa1ECz_FKVbYJkQ==";
    private static String dfltCalendarColor = "#0073b6";
    
    private AppProperties(){
    	InputStream input = null;
    	prop = new Properties();
		try {
			input = new FileInputStream(FILENAME);
			prop.load(input);
			
		} catch (IOException e) {
			// file not found, will used default
			// should put in the log
			System.out.println("No config.properties found, Using default settings.");
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
    }
     
    public static AppProperties getInstance(){
        if (instance == null) {
            instance = new AppProperties();
        }
        return instance;
    }
    
    public String getServerURI() {
    	return (prop.getProperty("serverURI") != null) ? prop.getProperty("serverURI") : serverURI;
    }
    
    public String getCndServerURI() {
    	return (prop.getProperty("cndServerURI") != null) ? prop.getProperty("cndServerURI") : cndServerURI;
    }
    
    public String getUsername() {
    	return (prop.getProperty("username") != null) ? prop.getProperty("username") : username;
    }
    
    public String getPwd() {
    	return (prop.getProperty("pwd") != null) ? prop.getProperty("pwd") : pwd;
    }
    
    public String getToken() {
    	return (prop.getProperty("token") != null) ? prop.getProperty("token") : token;
    }
    
    public String getInjestPath() {
    	return (prop.getProperty("injestPath") != null) ? prop.getProperty("injestPath") : injestPath;    
    }
    
    public String getArchivePath() {
    	return (prop.getProperty("archivePath") != null) ? prop.getProperty("archivePath") : archivePath;
    }
    
    public String getSelectClassCodes() {
    	return (prop.getProperty("selectClassCodes") != null) ? prop.getProperty("selectClassCodes") : selectClassCodes;
    }
    
    public String getDefaultCalendarColor() {
    	return (prop.getProperty("dfltCalendarColor") != null) ? prop.getProperty("dfltCalendarColor") : dfltCalendarColor;
    }
}
