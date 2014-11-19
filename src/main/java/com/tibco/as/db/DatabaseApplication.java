package com.tibco.as.db;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tibco.as.io.cli.Application;

public class DatabaseApplication extends Application {

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
		new DatabaseApplication().execute(args);
	}

	@Override
	protected void addCommands(JCommander jc) {
		jc.addCommand(new DatabaseImportCommand());
		jc.addCommand(new DatabaseExportCommand());
	}

	@Override
	protected String getProgramName() {
		return "as-db";
	}

	@Override
	protected DatabaseChannel getChannel(String metaspaceName) {
		DatabaseChannel channel = new DatabaseChannel(metaspaceName);
		channel.setConfigPath(config);
		channel.setDriver(driver);
		channel.setJar(jar);
		channel.setPassword(password);
		channel.setURL(url);
		channel.setUser(user);
		return channel;
	}

}
