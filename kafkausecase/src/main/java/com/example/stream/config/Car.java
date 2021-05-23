package com.example.stream.config;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.Module;
import org.codehaus.jackson.map.ObjectMapper;

public class Car {
	private String color;
    private String make;
    private String model;
    private String type;
    

    public Car() {
    	
    }
    @JsonCreator
    public static Car create(String jsonString) throws JsonParseException, JsonMappingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        Car car = null;
        car = mapper.readValue(jsonString, Car.class);
        return car;
    }
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getColor() {
		return color;
	}
	public void setColor(String color) {
		this.color = color;
	}
	public String getMake() {
		return make;
	}
	public void setMake(String make) {
		this.make = make;
	}
	public String getModel() {
		return model;
	}
	public void setModel(String model) {
		this.model = model;
	}
    

}
