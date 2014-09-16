package com.tibco.as.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXB;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tibco.as.io.cli.AbstractApplication;

public class Application extends AbstractApplication {

	@Parameter(names = { "-driver" }, description = "Database driver class name")
	private String driver;
	@Parameter(names = { "-url" }, description = "Database URL")
	private String url;
	@Parameter(names = { "-user" }, description = "Database user")
	private String user;
	@Parameter(names = { "-password" }, description = "Database password")
	private String password;
	@Parameter(names = { "-jar" }, description = "Path to driver JAR file")
	private String jar;
	@Parameter(names = { "-config" }, description = "Database configuration XML file")
	private String config;

	public static void main(String[] args) throws Exception {
		new Application().execute(args);
	}

	public Database getDatabase() throws FileNotFoundException {
		Database database;
		if (config == null) {
			database = new Database();
		} else {
			database = JAXB.unmarshal(new FileInputStream(config),
					Database.class);
		}
		if (database.getMetaspace() == null) {
			database.setMetaspace(getMetaspaceName());
		}
		if (database.getDriver() == null) {
			database.setDriver(driver);
		}
		if (database.getJar() == null) {
			database.setJar(jar);
		}
		if (database.getPassword() == null) {
			database.setPassword(password);
		}
		if (database.getUrl() == null) {
			database.setUrl(url);
		}
		if (database.getUser() == null) {
			database.setUser(user);
		}
		return database;
	}

	@Override
	protected void addCommands(JCommander jc) {
		jc.addCommand(new ImportCommand(this));
		jc.addCommand(new ExportCommand(this));
	}

	@Override
	protected String getProgramName() {
		return "as-db";
	}

}
