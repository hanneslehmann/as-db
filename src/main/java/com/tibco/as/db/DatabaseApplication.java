package com.tibco.as.db;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tibco.as.io.cli.AbstractCLIApplication;

public class DatabaseApplication extends AbstractCLIApplication {

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
		new DatabaseApplication().execute(args);
	}

	public Database getDatabase() {
		Database database = new Database();
		database.setDriver(driver);
		database.setJar(jar);
		database.setPassword(password);
		database.setUrl(url);
		database.setUser(user);
		return database;
	}

	@Override
	protected void addCommands(JCommander jc) {
		jc.addCommand(new DatabaseImportCommand(this));
		jc.addCommand(new DatabaseExportCommand(this));
	}

	@Override
	protected String getProgramName() {
		return "as-db";
	}

}
