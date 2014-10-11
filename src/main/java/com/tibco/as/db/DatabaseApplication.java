package com.tibco.as.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import javax.xml.bind.JAXB;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.cli.AbstractApplication;

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
	protected void addCommands(JCommander jc) {
		jc.addCommand(new DatabaseImportCommand());
		jc.addCommand(new DatabaseExportCommand());
	}

	@Override
	protected String getProgramName() {
		return "as-db";
	}

	@Override
	protected IChannel getChannel(ChannelConfig config) {
		return new DatabaseChannel((DatabaseConfig) config);
	}

	@Override
	protected DatabaseConfig getChannelConfig() throws FileNotFoundException {
		DatabaseConfig config = getDatabaseConfig();
		config.setDriver(driver);
		config.setJar(jar);
		config.setPassword(password);
		config.setURL(url);
		config.setUser(user);
		return config;
	}

	private DatabaseConfig getDatabaseConfig() throws FileNotFoundException {
		DatabaseConfig databaseConfig = new DatabaseConfig();
		if (config != null) {
			FileInputStream in = new FileInputStream(config);
			Database database = JAXB.unmarshal(in, Database.class);
			for (Table table : database.getTables()) {
				TableConfig tableConfig = new TableConfig();
				tableConfig.setCatalog(table.getCatalog());
				tableConfig.setCountSQL(table.getCountSQL());
				tableConfig.setInsertSQL(table.getInsertSQL());
				tableConfig.setTable(table.getName());
				tableConfig.setSchema(table.getSchema());
				tableConfig.setSelectSQL(table.getSelectSQL());
				tableConfig.setSpace(table.getSpace());
				tableConfig.setType(table.getType());
				for (Column column : table.columns) {
					ColumnConfig columnConfig = new ColumnConfig();
					columnConfig.setFieldName(column.getField());
					columnConfig.setColumnName(column.getName());
					columnConfig.setColumnNullable(column.isNullable());
					columnConfig.setColumnSize(column.getSize());
					columnConfig.setColumnType(column.getType());
					columnConfig.setDecimalDigits(column.getDecimalDigits());
					columnConfig.setKeySequence(column.getKeySequence());
					columnConfig.setRadix(column.getRadix());
					tableConfig.getFields().add(columnConfig);
				}
				databaseConfig.getDestinations().add(tableConfig);
			}
		}
		return databaseConfig;
	}
}
