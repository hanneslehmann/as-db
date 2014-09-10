package com.tibco.as.db;

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

	public static void main(String[] args) throws Exception {
		new Application().execute(args);
	}

	public Database getDatabase() {
		Database database = new Database();
		database.setMetaspace(getMetaspaceName());
		database.setDriver(driver);
		database.setJar(jar);
		database.setPassword(password);
		database.setUrl(url);
		database.setUser(user);
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
