package org.flowvisor.config;

import java.io.IOException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.flowvisor.api.APIAuth;
import org.flowvisor.exceptions.DuplicateControllerException;
import org.flowvisor.flows.FlowMap;
import org.flowvisor.log.FVLog;
import org.flowvisor.log.LogLevel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class SliceImpl implements Slice {
	
	private static SliceImpl instance = null;
	
	//Callbacks
	private static String FLLDP = "setLLDP";
	private static String FDROP = "setDropPolicy";
	private static String FHOST = "setControllerHost";
	private static String FPORT = "setControllerPort";
	private static String FFMLIMIT = "setFlowModLimit";
	
	// STATEMENTS
	private static String GALL = "SELECT S.*,F." + Flowvisor.CONFIG + " FROM Slice AS S, Flowvisor AS F WHERE S.flowvisor_id = F.id";
	private static String GLLDPSQL = "SELECT " + LLDP + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SLLDPSQL = "UPDATE Slice SET " + LLDP + "= ? WHERE " + SLICE + " = ?";
	private static String GDROPSQL = "SELECT " + DROP + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SDROPSQL = "UPDATE Slice SET " + DROP + "= ? WHERE " + SLICE + " = ?";
	private static String GHOSTSQL = "SELECT " + HOST + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SHOSTSQL = "UPDATE Slice SET " + HOST + "= ? WHERE " + SLICE + " = ?";
	private static String GPORTSQL = "SELECT " + PORT + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SPORTSQL = "UPDATE Slice SET " + PORT + "= ? WHERE " + SLICE + " = ?";
	private static String GPASSELM = "SELECT <REPLACEME> FROM Slice WHERE " + SLICE + " = ?";
	private static String GCREATOR = "SELECT " + CREATOR + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SEMAIL = "UPDATE Slice SET " + EMAIL + " = ? WHERE " + SLICE + " = ?";
	private static String GEMAIL = "SELECT " + EMAIL + " FROM Slice WHERE " + SLICE + " = ?";
	private static String SFMLIMIT = "UPDATE Slice SET " + FMLIMIT + " = ? WHERE " + SLICE + " = ?";
	private static String GFMLIMIT = "SELECT " + FMLIMIT + " FROM Slice WHERE " + SLICE + " = ?" ;
	private static String GALLSLICE = "SELECT " + SLICE + " FROM SLICE ORDER BY id ASC";
	private static String NAMECHECK = "SELECT id FROM Slice WHERE " + SLICE + " = ?";
	private static String CONTCHECK = "SELECT id from Slice WHERE " + HOST + " = ? AND " + PORT + " = ?"; 
	private static String CREATESLICE = "INSERT INTO Slice(flowvisor_id, flowmap_type, name, creator, passwd_crypt," +
			" passwd_salt, controller_hostname, controller_port, contact_email, drop_policy, lldp_spam, max_flow_rules) " +
			"VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String DELETESLICE = "DELETE FROM Slice WHERE " + SLICE + " = ?";
	
	private static String SADMINSTATUS = "UPDATE SLICE SET " + ADMINDOWN + " = ?" +
			" WHERE " + SLICE + " = ?";
	private static String SLICEDOWN = "SELECT " + ADMINDOWN +" FROM Slice WHERE " + SLICE + " = ?";
	
	
	private static String SCRYPT = "UPDATE Slice SET " + CRYPT + " = ?, " + SALT +
			" = ? WHERE " + SLICE + " = ?";
	
	
	private static String FLOWVISOR = "SELECT id from " + Flowvisor.FLOWVISOR + " WHERE " + Flowvisor.CONFIG + " = ?";
	

	private ConfDBSettings settings = null;
	
	
	public static SliceImpl getInstance() {
		if (instance == null)
			instance = new SliceImpl();
		return instance;
	}

	@Override
	public void setlldp_spam(String sliceName, Boolean LLDPSpam) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLLDPSQL);
			ps.setBoolean(1, LLDPSpam);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "LLDP update had no effect.");
			notify(sliceName, FLLDP, LLDPSpam);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	
	}
	
	@Override
	public void mongoSetLLDPSpam(String sliceName, Boolean LLDPSpam) {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + LLDP;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, LLDPSpam));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
			notify(sliceName, FLLDP, LLDPSpam);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void setdrop_policy(String sliceName, String policy) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SDROPSQL);
			ps.setString(1, policy);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Drop policy update had no effect.");
			notify(sliceName, FDROP, policy);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	
	}

	@Override
	public void mongoSetDropPolicy(String sliceName, String policy) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + DROP;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, policy));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
			notify(sliceName, FDROP, policy);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	
	@Override
	public void setcontroller_hostname(String sliceName, String name) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SHOSTSQL);
			ps.setString(1, name);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Controller host update had no effect.");
			notify(sliceName, FHOST, name);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update controller hostname for slice " + sliceName);
		} catch (NullPointerException npe) {
			npe.printStackTrace();
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}

	@Override
	public void mongoSetControllerHostname(String sliceName, String name) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + HOST;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, name));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
			notify(sliceName, FHOST, name);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update controller hostname for slice " + sliceName);
		} 
	}
	
	@Override
	public void setcontroller_port(String sliceName, Integer port) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SPORTSQL);
			ps.setInt(1, port);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Controller port update had no effect.");
			notify(sliceName, FPORT, port);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to set the controller port for slice " + sliceName);
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}

	}
	
	@Override
	public void mongoSetControllerPort(String sliceName, Integer port) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + PORT;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, port));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
			notify(sliceName, FPORT, port);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to set the controller port for slice " + sliceName);
		} 
	}
	
	@Override
	public void setContactEmail(String sliceName, String email) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SEMAIL);
			ps.setString(1, email);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				throw new ConfigError("Email for slice " + sliceName + " was not set to " + email);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void mongoSetContactEmail(String sliceName, String email) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + EMAIL;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, email));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		
	}
	
	@Override
	public void setPasswd(String sliceName, String salt, String crypt) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SCRYPT);
			ps.setString(1, crypt);
			ps.setString(2, salt);
			ps.setString(3, sliceName);
			if (ps.executeUpdate() == 0)
				throw new ConfigError("Password for slice " + sliceName + " was not updated");
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void mongoSetPasswd(String sliceName, String salt, String crypt) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateSalt = TSLICE + ".$." + SALT;
		String updateCrypt = TSLICE + ".$." + CRYPT;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject updateFields = new BasicDBObject();
			updateFields.put(updateSalt, salt);
			updateFields.put(updateCrypt, crypt);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", updateFields);
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
		} catch (UnknownHostException e) {
		}
	}
	
	
	@Override
	public void setMaxFlowMods(String sliceName, int limit) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SFMLIMIT);
			ps.setInt(1, limit);
			ps.setString(2, sliceName);
			if (ps.executeUpdate() == 0)
				throw new ConfigError("Global limit for slice " + sliceName + " was not set to " + limit);
			notify(sliceName, FFMLIMIT, limit);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
	}

	@Override
	public void mongoSetMaxFlowMods(String sliceName, int limit) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + FMLIMIT;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, limit));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
			/*
			BasicDBObject sliceQuery1 = new BasicDBObject();
			BasicDBObject sliceFields = new BasicDBObject().append(TSLICE, 1).append("_id", false); 
				
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery1, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					System.out.println(sliceSelect.next());
				}
			} finally {
				sliceSelect.close();
			}
			*/
			
			notify(sliceName, FFMLIMIT, limit);
			
		} catch (UnknownHostException e) {
			throw new ConfigError("Global limit for slice " + sliceName + " was not set to " + limit);
		} 
	}
	
	@Override
	public Integer getMaxFlowMods(String sliceName) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GFMLIMIT);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(FMLIMIT);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Integer mongoGetMaxFlowMods(String sliceName) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						throw new ConfigError("Max flow rules for slice named " + sliceName + " not found!");
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getInt(FMLIMIT);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	
	@Override
	public Boolean getlldp_spam(String sliceName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GLLDPSQL);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getBoolean(LLDP);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Boolean mongoGetLLDPSpam(String sliceName) {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						return true;
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getBoolean(LLDP);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		
		return null;
	}

	@Override
	public String getdrop_policy(String sliceName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GDROPSQL);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(DROP);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public String mongoGetDropPolicy(String sliceName) {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						return null;
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getString(DROP);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		
		return null;
	}
	
	@Override
	public String getcontroller_hostname(String sliceName) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GHOSTSQL);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(HOST);
			else
				throw new ConfigError("No such slice " + sliceName);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String mongoGetControllerHostname(String sliceName) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						throw new ConfigError("Controller host for slice " + sliceName + " not found.");
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getString(HOST);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}

	@Override
	public Integer getcontroller_port(String sliceName) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GPORTSQL);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(PORT);
			else
				throw new ConfigError("Controller port for slice " + sliceName + " not found.");
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Integer mongoGetControllerPort(String sliceName) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						throw new ConfigError("Controller port for slice " + sliceName + " not found!");
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getInt(PORT);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public String getPasswdElm(String sliceName, String elm) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			String stmt = GPASSELM.replaceFirst("<REPLACEME>", elm);
			conn = settings.getConnection();
			ps = conn.prepareStatement(stmt);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(elm);
			else
				throw new ConfigError("No " + elm + " found for " + sliceName);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@Override
	public String getCreator(String sliceName) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GCREATOR);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(CREATOR);
			else
				throw new ConfigError("Unknown slice " + sliceName);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String mongoGetCreator(String sliceName) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						throw new ConfigError("Unknown slice " + sliceName);
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getString(CREATOR);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		
		return null;
	}
	
	@Override
	public String getEmail(String sliceName) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GEMAIL);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(EMAIL);
			else
				throw new ConfigError("Unknown slice " + sliceName);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public String mongoGetEmail(String sliceName) throws ConfigError {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> queryResult = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					if (queryResult == null) 
						throw new ConfigError("Unknown slice " + sliceName);
					for (BasicDBObject sliceList: queryResult) {
						return sliceList.getString(EMAIL);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public LinkedList<String> getAllSliceNames() throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		LinkedList<String> list = new LinkedList<String>();
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GALLSLICE);
			set = ps.executeQuery();
			while (set.next())
				list.add(set.getString(SLICE));
			if (list.isEmpty())
				throw new ConfigError("No slices defined");
			return list;
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public LinkedList<String> mongoGetAllSliceNames() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String selectField = TSLICE + "." + SLICE;
		DB conn = null;
		DBCollection flowvisorColl = null;
		LinkedList<String> list = new LinkedList<String>();
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceFields = new BasicDBObject(selectField, 1).append("_id", false);
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> sliceList = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					for (BasicDBObject slice: sliceList) {
						list.add(slice.get(SLICE).toString());
					}
					if (list.isEmpty())
						throw new ConfigError("No slices defined");
					return list;
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public Boolean checkSliceName(String sliceName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(NAMECHECK);
			ps.setString(1, sliceName);
			return ps.execute();
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	@Override
	public Boolean mongoCheckSliceName(String sliceName) {
		
		String mongo;
		
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", new BasicDBObject(SLICE, sliceName));
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					if (sliceSelect.next().get(TSLICE) == null)
						return false;
					else
						return true;
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int flowvisor_id, int type) throws InvalidSliceName,
			DuplicateControllerException {
		createSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				APIAuth.getSalt(), sliceEmail, creatorSlice, flowvisor_id, type);
	}
	
	@Override
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int type) throws InvalidSliceName,
			DuplicateControllerException {
		String mongo;
		mongoCreateSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				APIAuth.getSalt(), sliceEmail, creatorSlice, type);
	}
	
	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int flowvisor_id) throws InvalidSliceName,
			DuplicateControllerException {
		createSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				APIAuth.getSalt(), sliceEmail, creatorSlice, flowvisor_id,
				FlowMap.type.FEDERATED.ordinal());

	}
	
	@Override
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice)
					throws InvalidSliceName, DuplicateControllerException {
		String mongo;
		mongoCreateSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				APIAuth.getSalt(), sliceEmail, creatorSlice, FlowMap.type.FEDERATED.ordinal());
	}

	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice) throws InvalidSliceName,
			DuplicateControllerException {
		createSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				APIAuth.getSalt(), sliceEmail, creatorSlice, 1,
				FlowMap.type.FEDERATED.ordinal());

	}
	
	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice)
			throws InvalidSliceName, DuplicateControllerException {
		createSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				salt, sliceEmail, creatorSlice, 1, FlowMap.type.FEDERATED.ordinal());
		
	}
	
	@Override
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice)
			throws InvalidSliceName, DuplicateControllerException {
		String mongo;
		mongoCreateSlice(sliceName, controllerHostname,
				controllerPort, dropPolicy, passwd,
				salt, sliceEmail, creatorSlice, FlowMap.type.FEDERATED.ordinal());
		
	}

	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, int flowvisor_id, int type)
			throws DuplicateControllerException {
		String crypt = APIAuth.makeCrypt(salt, passwd);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(CONTCHECK);
			ps.setString(1, controllerHostname);
			ps.setInt(2, controllerPort);
			set = ps.executeQuery();
			if (set.next())
				throw new DuplicateControllerException(controllerHostname, controllerPort, sliceName, null);
            close(conn);
			conn = settings.getConnection();
			ps = conn.prepareStatement(CREATESLICE);
			ps.setInt(1, flowvisor_id);
			ps.setInt(2, type);
			ps.setString(3, sliceName);
			ps.setString(4, creatorSlice);
			ps.setString(5, crypt);
			ps.setString(6, salt);
			ps.setString(7, controllerHostname);
			ps.setInt(8, controllerPort);
			ps.setString(9, sliceEmail);
			ps.setString(10, dropPolicy);
			ps.setBoolean(11, true);
			ps.setInt(12, -1);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Slice " + sliceName + " creation had no effect.");
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}	
	}
	
	@Override
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, int type)
			throws DuplicateControllerException {
		
		String mongo;	
		String crypt = APIAuth.makeCrypt(salt, passwd);
		String Flowvisor = "Flowvisor";
		String FSR = "FlowSpaceRule";
		String updateField = TSLICE;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			String clauseCondition1 = HOST;
			String clauseCondition2 = PORT;
			
			BasicDBObject searchQuery = new BasicDBObject();
			
			BasicDBObject queryClasue1 = new BasicDBObject(clauseCondition1, controllerHostname);
			BasicDBObject queryClasue2 = new BasicDBObject(clauseCondition2, controllerPort);
			
			BasicDBList compositeQuery = new BasicDBList();
			compositeQuery.add(queryClasue1);
			compositeQuery.add(queryClasue2);
			
			BasicDBObject sliceQuery = new BasicDBObject("$and", compositeQuery);
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", sliceQuery);
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(searchQuery, sliceFields);
			
			if (sliceSelect.next().get(TSLICE) == null) {
				
				BasicDBObject sliceSet = new BasicDBObject();
				
				sliceSet.put(EMAIL, sliceEmail);
				sliceSet.put(CREATOR, creatorSlice);
				sliceSet.put(SALT, salt);
				sliceSet.put(DROP, dropPolicy);
				sliceSet.put(FMLIMIT, -1);
				sliceSet.put(SLICE, sliceName);
				sliceSet.put(PORT, controllerPort);
				sliceSet.put(HOST, controllerHostname);
				sliceSet.put(FMTYPE, type); 	
				sliceSet.put(CRYPT, crypt);
				sliceSet.put(LLDP, true);
				List<BasicDBObject> FSRList = new ArrayList<BasicDBObject>();
				sliceSet.put(FSR, FSRList);
				
				BasicDBObject sliceUpdate = new BasicDBObject("$push", new BasicDBObject(updateField, sliceSet));
				flowvisorColl.update(searchQuery, sliceUpdate);
				
				sliceSelect.close();
				
			} else
				throw new DuplicateControllerException(controllerHostname, controllerPort, sliceName, null);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, boolean LLDPSpam, 
			int maxFlowMods, int flowvisor_id, int type)
			throws DuplicateControllerException {
		String crypt = APIAuth.makeCrypt(salt, passwd);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(CONTCHECK);
			ps.setString(1, controllerHostname);
			ps.setInt(2, controllerPort);
			set = ps.executeQuery();
			if (set.next())
				throw new DuplicateControllerException(controllerHostname, controllerPort, sliceName, null);
            close(conn);
			conn = settings.getConnection();
			ps = conn.prepareStatement(CREATESLICE);
			ps.setInt(1, flowvisor_id);
			ps.setInt(2, type);
			ps.setString(3, sliceName);
			ps.setString(4, creatorSlice);
			ps.setString(5, crypt);
			ps.setString(6, salt);
			ps.setString(7, controllerHostname);
			ps.setInt(8, controllerPort);
			ps.setString(9, sliceEmail);
			ps.setString(10, dropPolicy);
			ps.setBoolean(11, LLDPSpam);
			ps.setInt(12, maxFlowMods);
			if (ps.executeUpdate() == 0) {
				FVLog.log(LogLevel.WARN, null, "Slice " + sliceName + " creation had no effect.");
				return;
			}
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}	
	}

	@Override
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, 
			boolean LLDPSpam, int maxFlowMods, int type)
			throws DuplicateControllerException {
		
		String mongo;
		String crypt = APIAuth.makeCrypt(salt, passwd);
		String Flowvisor = "Flowvisor";
		String FSR = "FlowSpaceRule";
		String updateField = TSLICE;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			String clauseCondition1 = HOST;
			String clauseCondition2 = PORT;
			
			BasicDBObject searchQuery = new BasicDBObject();
			
			BasicDBObject queryClasue1 = new BasicDBObject(clauseCondition1, controllerHostname);
			BasicDBObject queryClasue2 = new BasicDBObject(clauseCondition2, controllerPort);
			
			BasicDBList compositeQuery = new BasicDBList();
			compositeQuery.add(queryClasue1);
			compositeQuery.add(queryClasue2);
			
			BasicDBObject sliceQuery = new BasicDBObject("$and", compositeQuery);
			BasicDBObject sliceMatch = new BasicDBObject("$elemMatch", sliceQuery);
			BasicDBObject sliceFields = new BasicDBObject(TSLICE, sliceMatch).append("_id", false); 
			
			DBCursor sliceSelect = flowvisorColl.find(searchQuery, sliceFields);
			
			if (sliceSelect.next().get(TSLICE) == null) {
				
				BasicDBObject sliceSet = new BasicDBObject();
				
				sliceSet.put(EMAIL, sliceEmail);
				sliceSet.put(CREATOR, creatorSlice);
				sliceSet.put(SALT, salt);
				sliceSet.put(DROP, dropPolicy);
				sliceSet.put(FMLIMIT, maxFlowMods);
				sliceSet.put(SLICE, sliceName);
				sliceSet.put(PORT, controllerPort);
				sliceSet.put(HOST, controllerHostname);
				sliceSet.put(FMTYPE, type); 	
				sliceSet.put(CRYPT, crypt);
				sliceSet.put(LLDP, LLDPSpam);
				List<BasicDBObject> FSRList = new ArrayList<BasicDBObject>();
				sliceSet.put(FSR, FSRList);
				
				BasicDBObject sliceUpdate = new BasicDBObject("$push", new BasicDBObject(updateField, sliceSet));
				flowvisorColl.update(searchQuery, sliceUpdate);
				
				sliceSelect.close();
				
			} else
				throw new DuplicateControllerException(controllerHostname, controllerPort, sliceName, null);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void deleteSlice(String sliceName, Boolean preserve) 
			throws InvalidSliceName, ConfigError {
		if (preserve) 
			FlowSpaceImpl.getProxy().saveFlowSpace(sliceName);
		deleteSlice(sliceName);
	}
	
	@Override
	public void deleteSlice(String sliceName) throws InvalidSliceName {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(DELETESLICE);
			ps.setString(1, sliceName);
			if (ps.executeUpdate() == 0)
				throw new InvalidSliceName("Unknown slice name : " + sliceName);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}
	}
	
	@Override
	public void mongoDeleteSlice(String sliceName) throws InvalidSliceName {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject(TSLICE, new BasicDBObject(SLICE, sliceName));
			flowvisorColl.update(sliceQuery, new BasicDBObject("$pull", sliceUpdate));
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void setAdminStatus(String sliceName, boolean status) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SADMINSTATUS);
			ps.setBoolean(1, status);
			ps.setString(2, sliceName);
			ps.executeUpdate();
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}
	}
	
	@Override
	public void mongoSetAdminStatus(String sliceName, boolean status) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String queryCondition = TSLICE + "." + SLICE;
		String updateField = TSLICE + ".$." + ADMINDOWN;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceUpdate = new BasicDBObject("$set", new BasicDBObject(updateField, status));
			flowvisorColl.update(sliceQuery, sliceUpdate);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	 
	@Override
	public boolean isSliceUp(String sliceName) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLICEDOWN);
			ps.setString(1, sliceName);
			set = ps.executeQuery();
			if (set.next()) 
				return set.getBoolean(ADMINDOWN);
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
		}
		return false;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public boolean mongoIsSliceUp(String sliceName) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String selectField = TSLICE + "." + ADMINDOWN;
		String queryCondition = TSLICE + "." + SLICE;
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject sliceQuery = new BasicDBObject(queryCondition, sliceName);
			BasicDBObject sliceFields = new BasicDBObject(selectField, 1).append("_id", false);
			
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
					ArrayList<BasicDBObject> sliceList = (ArrayList<BasicDBObject>) sliceSelect.next().get(TSLICE);
					for (BasicDBObject slice: sliceList) {
						return slice.getBoolean(ADMINDOWN);
					}
				}
			} finally {
				sliceSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return false;
	}
	
	
	@Override
	public void close(Connection conn) {
		//settings.returnConnection(conn);
		try {
			conn.close();
		} catch (Exception e) {
			// don't care
		}
	}
	
	@Override
	public void close(Object o) {
		try {
			o.getClass().getMethod("close", (Class<?>) null).invoke(null,(Object[]) null);
		} catch (Exception e) {
			// Don't care, haha!
		}
	}

	@Override
	public void notify(Object key, String method, Object newValue) {
		FVConfigurationController.instance().fireChange(key, method, newValue);
		
	}


	@Override
	public void setSettings(ConfDBSettings settings) {
		this.settings = settings;
	}

	public static Slice getProxy() {
		return (Slice) FVConfigurationController.instance()
		.getProxy(getInstance());
	}
	
	public static void addListener(String sliceName, SliceChangedListener l) {
		FVConfigurationController.instance().addChangeListener(sliceName, l);
	}
	
	public static void removeListener(String sliceName, FlowvisorChangedListener l) {
		FVConfigurationController.instance().removeChangeListener(sliceName, l);
	}

	@Override
	public HashMap<String, Object> toJson(HashMap<String, Object> output) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;	
		HashMap<String, Object> slice = new HashMap<String, Object>();
		LinkedList<Object> list = new LinkedList<Object>();
				
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GALL);
			set = ps.executeQuery();
			while (set.next()) {
				slice.put(Flowvisor.CONFIG, set.getString(Flowvisor.CONFIG));
				slice.put(FMTYPE, FlowMap.type.values()[set.getInt(FMTYPE)].getText());
				slice.put(SLICE, set.getString(SLICE));
				slice.put(CREATOR, set.getString(CREATOR));
				slice.put(CRYPT, set.getString(CRYPT));
				slice.put(SALT, set.getString(SALT));
				slice.put(HOST, set.getString(HOST));
				slice.put(PORT, set.getInt(PORT));
				slice.put(EMAIL, set.getString(EMAIL));
				slice.put(DROP, set.getString(DROP));
				slice.put(LLDP, set.getBoolean(LLDP));
				slice.put(FMLIMIT, set.getInt(FMLIMIT));
				slice.put(ADMINDOWN, set.getBoolean(ADMINDOWN));
				
				list.add(slice.clone());
				slice.clear();
				
			}
			output.put(TSLICE, list);	
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, "Failed to write slice information "  + e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
	
		return output;
	}
	
	@Override
	public HashMap<String, Object> mongoToJson(HashMap<String, Object> output) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		String Slice = "Slice"; // This will go once we have consistency in the naming
		DB conn = null;
		DBCollection flowvisorColl = null;
		LinkedList<Object> list = new LinkedList<Object>();

		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
	
			BasicDBObject sliceQuery = new BasicDBObject();
			BasicDBObject sliceFields = new BasicDBObject().append(Slice, 1).append("_id", false);
			DBCursor sliceSelect = flowvisorColl.find(sliceQuery, sliceFields);
			
			try {
				while (sliceSelect.hasNext()) {
				
					BasicDBList e = (BasicDBList) sliceSelect.next().get("Slice");
				
					for (int i = 0; i < e.size(); i++) {
						
						list.add(e.get(i));
						//System.out.println(e.get(i));
					}
					
				}
			} finally {
				sliceSelect.close();
			}
			output.put(Slice, list);
			//System.out.println("list: " + list);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, "Failed to write mongo Slice info : "  + e.getMessage());
		}
		return output;
	}

	@Override
	public void fromJson(ArrayList<HashMap<String, Object>> list) throws IOException {
		for (HashMap<String, Object> row : list)
			insert(row);
	}
	
	private void insert(HashMap<String, Object> row) throws IOException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		int flowvisorid = -1;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(FLOWVISOR);
			ps.setString(1, (String) row.get(Flowvisor.CONFIG));
			set = ps.executeQuery();
			if (set.next())
				flowvisorid = set.getInt("id");
			else
				throw new IOException("Unknown config name " + row.get(Flowvisor.CONFIG));
			ps = conn.prepareStatement(CREATESLICE);
			ps.setInt(1, flowvisorid);
			ps.setInt(2, FlowMap.type.fromString((String) row.get(FMTYPE)).ordinal());
			ps.setString(3, (String) row.get(SLICE));
			ps.setString(4, (String) row.get(CREATOR));
			ps.setString(5, (String) row.get(CRYPT));
			ps.setString(6, (String) row.get(SALT));
			ps.setString(7, (String) row.get(HOST));
			ps.setInt(8, ((Double) row.get(PORT)).intValue());
			ps.setString(9, (String) row.get(EMAIL));
			if (row.get(DROP) == null)
				row.put(DROP, "exact");
			ps.setString(10, (String) row.get(DROP));
			if (row.get(LLDP) == null)
				row.put(LLDP, true);
			ps.setBoolean(11, (Boolean) row.get(LLDP));
			if (row.get(FMLIMIT) != null)
				ps.setInt(12, ((Double) row.get(FMLIMIT)).intValue());
			else
				ps.setInt(12, -1);
			if (ps.executeUpdate() == 0) {
				FVLog.log(LogLevel.WARN, null, "Insertion failed... siliently.");
				return;
			}
			if (row.get(ADMINDOWN) != null)
				setAdminStatus((String) row.get(SLICE), (Boolean) row.get(ADMINDOWN));
			else 
				setAdminStatus((String) row.get(SLICE), true);
			} catch (SQLException e) {
				e.printStackTrace();
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void mongoFromJson(ArrayList<HashMap<String, Object>> list)
			throws IOException {
		
		String mongo;
		String Slice = "Slice"; // This will go once we have consistency in the naming
		DB conn = null;
		DBCollection sliceColl = null;
		
	try {	
			conn = settings.getMongoConnection();
			sliceColl = conn.getCollection(Slice);
			
			BasicDBObject sliceInsert = new BasicDBObject();
			
			for (HashMap<String, Object> row : list) {
				
				sliceInsert.put(EMAIL, (String) row.get(EMAIL));
				
				if (row.get(ADMINDOWN) != null)
					sliceInsert.put((String) row.get(SLICE), (Boolean) row.get(ADMINDOWN));
				else
					sliceInsert.put((String) row.get(SLICE), true);
			
				sliceInsert.put(CREATOR, (String) row.get(CREATOR));
				sliceInsert.put(SALT, (String) row.get(SALT));
				
				if (row.get(DROP) == null)
					row.put(DROP, "exact");
				sliceInsert.put(DROP, (String) row.get(DROP));
				
				sliceInsert.put(Flowvisor.CONFIG, (String) row.get(Flowvisor.CONFIG));
				
				if (row.get(FMLIMIT) != null)
					sliceInsert.put(FMLIMIT, ((Double) row.get(FMLIMIT)).intValue());
				else 
					sliceInsert.put(FMLIMIT, -1);
					
				sliceInsert.put(SLICE, (String) row.get(SLICE));
				sliceInsert.put(PORT, ((Double) row.get(PORT)).intValue());
				sliceInsert.put(HOST, (String) row.get(HOST));
				sliceInsert.put(FMTYPE, (String) row.get(FMTYPE));
				sliceInsert.put(CRYPT, (String) row.get(CRYPT));
				
				if (row.get(LLDP) == null) 
					row.put(LLDP, true);
				sliceInsert.put(LLDP, (Boolean)row.get(LLDP));	
			}
			
			sliceColl.insert(sliceInsert);
			DBObject query = sliceColl.findOne();
			System.out.println(query);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, "Failed to write mongo Slice info : "  + e.getMessage());
		}
	}

	private void processAlter(String alter) {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(alter);
			ps.execute();
		} catch (SQLException e) {
			System.err.println("WARN: " + e.getMessage());
		} finally {
			close(ps);
			close(conn);
		}
	}

	@Override
	public void updateDB(int version) {
		FVLog.log(LogLevel.INFO, null, "Updating Slice database table.");
		if (version == 0) {
			processAlter("ALTER TABLE Slice ADD COLUMN " + FMLIMIT + " INT NOT NULL DEFAULT -1");
			version++;
		}
		if (version == 1) {
			processAlter("ALTER TABLE Slice ADD COLUMN " + ADMINDOWN + " BOOLEAN NOT NULL DEFAULT TRUE");
			version++;
		}
		
		
	}
}
