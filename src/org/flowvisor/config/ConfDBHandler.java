package org.flowvisor.config;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.flowvisor.log.FVLog;
import org.flowvisor.log.LogLevel;

import com.mongodb.MongoClient;

import com.mongodb.DB;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;


/**
 * Defines a connection pool to derby. 
 * Guarantees that the status of a returned connection 
 * is valid.
 * 
 * 
 * @author ash
 *
 */
public class ConfDBHandler implements ConfDBSettings {
	
	private String dbName = null;
	private String mongoDBName = "FlowVisorDB";

	private EmbeddedConnectionPoolDataSource pds = null;
	private MongoClient mongoClient = null;
			
	public ConfDBHandler(String dbName) {
		this.dbName = System.getProperty("derby.system.home") + "/" + dbName;
	}
	
	public ConfDBHandler() {
		this("FlowVisorDB");
	}
	
	private DataSource getDataSource() {
		if (pds != null) 
			return pds;
		
		pds = new EmbeddedConnectionPoolDataSource();
		pds.setDatabaseName(this.dbName);
		
		return pds;
	}

	@Override
	public Connection getConnection() throws SQLException {
		return getDataSource().getConnection();		
	}

	@Override
	public Connection getConnection(String user, String pass)
			throws SQLException {
		return getDataSource().getConnection(user, pass);
	}
	

	@Override
	public void shutdown() {
		try {
			//gop.close();
			((EmbeddedDataSource) getDataSource()).setShutdownDatabase("shutdown");
		} catch (ClassCastException cce) {
			//Isn't this a derby db?
		} catch (Exception e) {
			FVLog.log(LogLevel.WARN, null, "Error on closing connection pool to derby");
		}
	}

	///*
	@Override
	public DB getMongoConnection() throws UnknownHostException {
		
		MongoClientOptions clientOptions = new MongoClientOptions.Builder().build();
		mongoClient = new MongoClient(new ServerAddress("localhost"), clientOptions);
	
		DB db = mongoClient.getDB(mongoDBName);
	
		// TODO Auto-generated method stub
		return db;
	}
	//*/
	
	///*
	@Override
	public void mongoShutdown() {
		//System.out.println("We are about to close MongoDB...");
		mongoClient.close();
		// TODO Auto-generated method stub	
	}
	//*/
}
