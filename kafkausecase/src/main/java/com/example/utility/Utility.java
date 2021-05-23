package com.example.utility;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.jdbc2.optional.SimpleDataSource;

import com.example.stream.config.Car;

public class Utility {
	 public static String readFileAsString(String file)throws Exception
	    {
	        return new String(Files.readAllBytes(Paths.get(file)));
	    }
	 public static List<String> readAllFileAsString(String directory)throws Exception {
		 File directoryPath = new File(directory);
	      //List of all files and directories
	      File filesList[] = directoryPath.listFiles();
	      List<String> list = new ArrayList<String>();
	      if(null != filesList) {
	    	  for(File file:filesList) {
	    		  list.add(new String(Files.readAllBytes(Paths.get(file.getAbsolutePath()))));
	    	  }
	      }
	      return list;
	}
	 
	public static List<String> getJsonValuesFromFile(){
		List<String> lines = new ArrayList<>();
		try {
			String fileName = "InputMessages.txt";
			ClassLoader loader = Utility.class.getClassLoader();
			File file = new File(loader.getResource(fileName).getFile());
			lines = Files.lines(file.toPath()).collect(Collectors.toList());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;
		   
	}
	
	 public static void main(String[] args) throws Exception {
		List<String> list = getJsonValuesFromFile();
		System.out.println(list);
		 for(String value : list) {
			 Car car = Car.create(value);
			 System.out.println(car.getColor());
		 }
	}
}
