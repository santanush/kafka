package com.example.utility;

import java.sql.Connection;
import java.sql.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class DatabaseUtility {
	private static Connection connection = null;
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseUtility.class);
	
	public static Connection getConnection() {
		try {
			if(null == connection) {
		         Class.forName("org.postgresql.Driver");
		         String url = new StringBuffer().append("jdbc:postgresql://")
		        		      .append(PropertiesUtil.getPropertyValue("metadata.database.url"))
		        		      .append(":")
		        		      .append(PropertiesUtil.getPropertyValue("metadata.database.port"))
		        		      .append("/")
		        		      .append(PropertiesUtil.getPropertyValue("metadata.database.db"))
		        		      .toString();
		         System.out.println("URL is ::::"+url);
		         System.out.println("URL is ::::"+PropertiesUtil.getPropertyValue("metadata.database.db.user"));
		         System.out.println("URL is ::::"+PropertiesUtil.getPropertyValue("metadata.database.password"));
		         connection = DriverManager
		            .getConnection(url,
		            		PropertiesUtil.getPropertyValue("metadata.database.user"),
		            		   PropertiesUtil.getPropertyValue("metadata.database.password"));
			}
	      } catch (Exception e) {
	         LOGGER.error(e.getMessage(),e);
	        
	      }
		return connection;
	}

}
