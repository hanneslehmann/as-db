package com.tibco.as.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.JAXB;

import com.tibco.as.io.Channel;
import com.tibco.as.io.Destination;
import com.tibco.as.log.LogFactory;

public class DatabaseChannel extends Channel {

	private Logger log = LogFactory.getLog(DatabaseChannel.class);
	private String configPath;
	private String driver;
	private String jar;
	private String url;
	private String user;
	private String password;
	private Connection connection;

	public DatabaseChannel(String metaspaceName) {
		super(metaspaceName);
	}

	public String getConfigPath() {
		return configPath;
	}

	public void setConfigPath(String configPath) {
		this.configPath = configPath;
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

	@Override
	public void open() throws Exception {
		loadConfig();
		log.info("Connecting to database");
		Properties props = new Properties();
		if (user != null) {
			props.put("user", user);
		}
		if (password != null) {
			props.put("password", password);
		}
		if (driver == null) {
			connection = DriverManager.getConnection(url, props);
		} else {
			Driver driver = getDriverClass().newInstance();
			connection = driver.connect(url, props);
		}
		log.finest("Setting the connection's commit mode to auto");
		connection.setAutoCommit(true);
		super.open();
	}

	private void loadConfig() throws FileNotFoundException {
		if (configPath == null) {
			return;
		}
		FileInputStream in = new FileInputStream(configPath);
		Database database = JAXB.unmarshal(in, Database.class);
		for (Table table : database.getTables()) {
			addDestination(new TableDestination(this, table));
		}
	}

	public Connection getConnection() {
		return connection;
	}

	@SuppressWarnings("unchecked")
	private Class<Driver> getDriverClass() throws MalformedURLException,
			ClassNotFoundException {
		if (jar == null) {
			return (Class<Driver>) Class.forName(driver);
		}
		URL[] urls = { new URL("file://" + jar) };
		URLClassLoader classLoader = URLClassLoader.newInstance(urls);
		log.log(Level.FINE, "Loading driver ''{0}'' from {1}", new Object[] {
				driver, Arrays.toString(urls) });
		return (Class<Driver>) classLoader.loadClass(driver);
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (connection == null) {
			return;
		}
		log.info("Disconnecting from database");
		connection.close();
		connection = null;
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return connection.getMetaData();
	}

	@Override
	public TableDestination newDestination() {
		return new TableDestination(this, new Table());
	}

	@Override
	protected Collection<Destination> getImportDestinations(
			Destination destination) throws SQLException {
		TableDestination tableDestination = (TableDestination) destination;
		Collection<Destination> destinations = new ArrayList<Destination>();
		for (Table table : tableDestination.getTables()) {
			destinations.add(new TableDestination(this, table));
		}
		return destinations;
	}

	public void setTableNames(Collection<String> tableNames) {
		for (String tableName : tableNames) {
			Table table = new Table();
			table.setName(tableName);
			addDestination(new TableDestination(this, table));
		}
	}

}
