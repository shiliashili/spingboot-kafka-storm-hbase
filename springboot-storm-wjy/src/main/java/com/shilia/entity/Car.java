package com.shilia.entity;

public class Car {
	private String usr;
	private String id;
	private String name;
	public String getUsr() {
		return usr;
	}
	public void setUsr(String usr) {
		this.usr = usr;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	@Override
	public String toString() {
		return "Car [usr=" + usr + ", id=" + id + ", name=" + name + "]";
	}
	
	
}
