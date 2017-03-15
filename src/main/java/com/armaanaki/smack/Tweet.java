
package com.armaanaki.smack;

import java.util.Date;

public class Tweet implements java.io.Serializable{
	private static final long serialVersionUID = -2599376376240068235L;
	private Date date;
	private String text;
	
	public Tweet() {
	}
	
	public Tweet(Date date, String text) {
		this.date = date;
		this.text = text;
	}
	
	public Date getDate() {
		return date;
	}
	
	public String getText() {
		return text;
	}
	
	public String toString() {
		return date + " " + text;
	}
}
