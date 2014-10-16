package com.tibco.as.db;

import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.DestinationConfig;

public class DatabaseConfig extends ChannelConfig {

	private String driver;
	private String jar;
	private String url;
	private String user;
	private String password;
	
	@Override
	protected DestinationConfig newDestinationConfig() {
		return new TableConfig(this);
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getJar() {
		return jar;
	}

	public void setJar(String jar) {
		this.jar = jar;
	}

	public String getURL() {
		return url;
	}

	public void setURL(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
