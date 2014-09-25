package com.tibco.as.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXB;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.AbstractApplication;
import com.tibco.as.space.Metaspace;

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
	@Parameter(names = { "-keep_open" }, description = "Do not close database connection after execution", hidden = true)
	private boolean keepOpen;

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
	protected IChannel getChannel(Metaspace metaspace)
			throws FileNotFoundException {
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
		DatabaseChannel channel = new DatabaseChannel(metaspace, database);
		for (Table table : database.getTables()) {
			TableConfig config = new TableConfig();
			config.setTable(table);
			channel.addConfig(config);
		}
		channel.setKeepOpen(keepOpen);
		return channel;
	}

}
