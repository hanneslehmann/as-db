package com.tibco.as.db.cli;

import java.util.Arrays;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.db.DatabaseChannel;
import com.tibco.as.io.cli.AbstractApplication;
import com.tibco.as.io.cli.ICommand;

public class DatabaseApplication extends AbstractApplication {

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
	protected Collection<ICommand> getCommands() {
		return Arrays.asList((ICommand) new DatabaseImportCommand(),
				new DatabaseExportCommand(), new DatabaseSNExportCommand());
	}

	@Override
	protected String getProgramName() {
		return "as-db";
	}

	@Override
	protected DatabaseChannel getChannel() {
		DatabaseChannel channel = new DatabaseChannel();
		channel.setConfigPath(config);
		channel.setDriver(driver);
		channel.setJar(jar);
		channel.setPassword(password);
		channel.setURL(url);
		channel.setUser(user);
		return channel;
	}

}
