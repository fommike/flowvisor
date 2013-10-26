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

import org.flowvisor.FlowVisor;
import org.flowvisor.api.APIAuth;
import org.flowvisor.log.FVLog;
import org.flowvisor.log.LogLevel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;



public class FlowvisorImpl implements Flowvisor {

	private static FlowvisorImpl instance = null;
	
	private ConfDBSettings settings = null;
	
	// Callbacks
	private static String FTRACK = "setFlowTracking";
	private static String FSTATS = "setStatsDescHack";
	private static String FFLOOD = "setFloodPerm";
	
	// STATEMENTS
	private static String GALL = "SELECT * FROM Flowvisor";
	private static String GTRACKID = "SELECT " + TRACK + " FROM Flowvisor WHERE id = ?";
	private static String GSTATSID = "SELECT " + STATS + " FROM Flowvisor WHERE id = ?";
	private static String GAPIPORT = "SELECT " + APIPORT + " FROM Flowvisor WHERE id = ?";
	private static String GLISTEN = "SELECT " + LISTEN + " FROM FlowVisor WHERE id = ?";
	private static String GJETTYPORT = "SELECT " + JETTYPORT + " FROM Flowvisor WHERE id = ?";
	private static String GFLOODPERM = "SELECT " + FLOODPERM + " FROM Flowvisor WHERE id = ?";
 	private static String GCHECKPOINT = "SELECT " + CHECKPOINT+ " FROM FlowVisor WHERE id = ?";
 	private static String GLOGIDENT = "SELECT " + LOGIDENT + " FROM FlowVisor WHERE id = ?";
 	private static String GLOGGING = "SELECT " + LOGGING + " FROM FlowVisor WHERE id = ?";
 	private static String GLOGFACILITY = "SELECT " + LOGFACILITY + " FROM FlowVisor WHERE id = ?";
 	private static String GTOPO = "SELECT " + TOPO + " FROM Flowvisor WHERE id = ?";
 	private static String GFSTIME = "SELECT " + FSCACHE + " FROM Flowvisor WHERE id = ?";
 	
 	
 	private static String STRACKID = "UPDATE Flowvisor SET " + TRACK + " = ? WHERE id = ?";
	private static String SSTATSID = "UPDATE Flowvisor SET " + STATS + " = ? WHERE id = ?";
	private static String SFLOODPERM = "UPDATE Flowvisor SET " + FLOODPERM + " = ? WHERE id = ?";
	private static String SLOGGING = "UPDATE Flowvisor SET " + LOGGING + " = ? WHERE id = ?";
	private static String SLOGFACILITY = "UPDATE Flowvisor SET " + LOGFACILITY + " = ? WHERE id = ?";
	private static String SLOGIDENT  = "UPDATE Flowvisor SET " + LOGIDENT + " = ? WHERE id = ?";
	private static String STOPO = "UPDATE Flowvisor SET " + TOPO + " = ? WHERE id = ?";
	private static String SLISTEN = "UPDATE Flowvisor SET " + LISTEN + " = ? WHERE id = ?";
	private static String SAPIPORT = "UPDATE Flowvisor SET " + APIPORT + " = ? WHERE id = ?";
	private static String SJETTYPORT = "UPDATE Flowvisor SET " + JETTYPORT + " = ? WHERE id = ?";
	private static String SFSTIME = "UPDATE Flowvisor SET " + FSCACHE + " = ? WHERE id = ?";
	
	private static String INSERT = "INSERT INTO " + FLOWVISOR + "(" + APIPORT + "," + 
					JETTYPORT + "," + CHECKPOINT + "," + LISTEN + "," + TRACK + "," +
					STATS + "," + TOPO + "," + LOGGING + "," + LOGIDENT + "," + LOGFACILITY + "," +
					VERSION + "," + HOST + "," + FLOODPERM + "," + CONFIG + "," + DB_VERSION + ") VALUES(" +
					"?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	private static String DELETE = "DELETE FROM " + FLOWVISOR;
	private static String DELSWITCH = "DELETE FROM " + Switch.TSWITCH;
	private static String RESETFLOWVISOR = "ALTER TABLE Flowvisor ALTER COLUMN id RESTART WITH 1";
	private static String RESETSWITCH = "ALTER TABLE Switch ALTER COLUMN id RESTART WITH 1";
	
	
	private FlowvisorImpl() {}
	
	private static FlowvisorImpl getInstance() {
		if (instance == null)
			instance = new FlowvisorImpl();
		return instance;
	}
	
	@Override
	public Boolean gettrack_flows(Integer id) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GTRACKID);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getBoolean(TRACK);
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
	public Boolean gettrack_flows() throws ConfigError {
		return gettrack_flows(1);
	}
	
	@Override
	public Boolean mongoGetTrackFlows() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(TRACK, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Boolean queryResult = (Boolean) flowvisorSelect.next().get(TRACK);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Track flows not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public Integer getListenPort(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GLISTEN);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(LISTEN);
			else
				throw new ConfigError("Listen port not found");
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
	public Integer getListenPort() throws ConfigError {
		return getListenPort(1);
	}
	
	@Override
	public Integer mongoGetListenPort() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(LISTEN, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Integer queryResult = (Integer) flowvisorSelect.next().get(LISTEN);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Listen port not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public Boolean getCheckPoint(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GCHECKPOINT);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getBoolean(CHECKPOINT);
			else
				throw new ConfigError("CheckPointing config not found");
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	public Boolean getCheckPoint() throws ConfigError {
		return getCheckPoint(1);
	}
	
	@Override
	public Boolean mongoGetCheckPoint() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(CHECKPOINT, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Boolean queryResult = (Boolean) flowvisorSelect.next().get(CHECKPOINT);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Checkpointing not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}

	@Override
	public Boolean getstats_desc_hack(Integer id) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GSTATSID);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getBoolean(STATS);
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
	public Boolean getstats_desc_hack() {
		return getstats_desc_hack(1);
	}
	
	@Override
	public Boolean mongoGetStatsDescHack() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(STATS, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Boolean queryResult = (Boolean) flowvisorSelect.next().get(STATS);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Stats desc hack not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	public Integer getAPIWSPort(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GAPIPORT);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(APIPORT);
			else
				throw new ConfigError("API Webserver port not found");
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			
			close(set);
			close(ps);
			close(conn);
			
		}
		return null;
	}
	
	public Integer getAPIWSPort() throws ConfigError {
		return getAPIWSPort(1);
	}

	@Override
	public Integer mongoGetAPIWSPort() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(APIPORT, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Integer queryResult = (Integer) flowvisorSelect.next().get(APIPORT);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("API Webserver port not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	
	public Integer getJettyPort(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GJETTYPORT);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(JETTYPORT);
			else
				throw new ConfigError("API Jetty Webserver port not found");
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
	public Integer getJettyPort() throws ConfigError {
		return getJettyPort(1);
	}
	
	@Override
	public Integer mongoGetJettyPort() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(JETTYPORT, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Integer queryResult = (Integer) flowvisorSelect.next().get(JETTYPORT);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("API Jetty Webserver port not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public String getFloodPerm(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GFLOODPERM);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(FLOODPERM);
			else
				throw new ConfigError("default flood permissions not found");
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
	public String getFloodPerm() throws ConfigError {
		return getFloodPerm(1);
	}
	
	@Override
	public String mongoGetFloodPerm() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(FLOODPERM, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					String queryResult = (String) flowvisorSelect.next().get(FLOODPERM);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Default flood permissions not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public String getLogIdent(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GLOGIDENT);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(LOGIDENT);
			else
				throw new ConfigError("Log ident not found");
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
	public String getLogIdent() throws ConfigError{
		return getLogIdent(1);
	}
	
	@Override
	public String mongoGetLogIdent() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(LOGIDENT, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					String queryResult = (String) flowvisorSelect.next().get(LOGIDENT);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Log indent not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	
	@Override
	public String getLogging(Integer id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GLOGGING);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(LOGGING);
			else
				throw new ConfigError("logging not found");
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw new ConfigError(e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
	}
	
	@Override
	public String getLogging() throws ConfigError{
		return getLogging(1);
	}
	
	@Override
	public String mongoGetLogging() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(LOGGING, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					String queryResult = (String) flowvisorSelect.next().get(LOGGING);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Logging not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public String getLogFacility(Integer id) throws ConfigError{
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GLOGFACILITY);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getString(LOGFACILITY);
			else
				throw new ConfigError("Log facility not found");
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
	public String getLogFacility() throws ConfigError {
		return getLogFacility(1);
	}
	
	@Override
	public String mongoGetLogFacility() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(LOGFACILITY, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					String queryResult = (String) flowvisorSelect.next().get(LOGFACILITY);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Log facility found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	@Override
	public Boolean getTopologyServer(int id) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GTOPO);
			ps.setInt(1, id);
			set = ps.executeQuery();
			if (set.next())
				return set.getBoolean(TOPO);
			else
				throw new ConfigError("Topology Server not found");
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
	public Boolean getTopologyServer() throws ConfigError {
		return getTopologyServer(1);
	}
	
	@Override
	public Boolean mongoGetTopologyServer() throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(TOPO, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Boolean queryResult = (Boolean) flowvisorSelect.next().get(TOPO);
					if (queryResult != null)
						return queryResult;
					else
						throw new ConfigError("Topology server not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}
	
	
	@Override
	public Integer getFlowStatsCache() throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GFSTIME);
			ps.setInt(1, 1);
			set = ps.executeQuery();
			if (set.next())
				return set.getInt(FSCACHE);
			else
				throw new ConfigError("Flowstats cache timeout value not found.");
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
	public Integer mongoGetFlowStatsCache() throws ConfigError {
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(FSCACHE, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Integer queryResult = (Integer) flowvisorSelect.next().get(FSCACHE);
					if (queryResult != null) 
						return queryResult;
					else 
						throw new ConfigError("Flowstats cache timeout value not found!");
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return null;
	}

	@Override
	public void setFlowStatsCache(Integer timer) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SFSTIME);
			ps.setInt(1, timer);
			ps.setInt(2, 1);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Flow stats cache timeout setting update had no effect.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update flow stats timer setting.");
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	
	}
	
	@Override
	public void mongoSetFlowStatsCache(Integer timer) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(FSCACHE, timer));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update flow stats timer setting!");
		} 
		
	}
	
	@Override
	public void setTopologyServer(Integer id, Boolean topo) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(STOPO);
			ps.setBoolean(1, topo);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Topology server setting update had no effect.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update topology server setting.");
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	

	}
	
	@Override
	public void setTopologyServer(Boolean topo) throws ConfigError {
		setTopologyServer(1, topo);
	}
	
	@Override
	public void mongoSetTopologyServer(Boolean topo) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(TOPO, topo));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			throw new ConfigError("Unable to update topology server setting!");
		} 
	}
	
	@Override
	public void setFloodPerm(Integer id, String floodPerm) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SFLOODPERM);
			ps.setString(1, floodPerm);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Track flows update had no effect.");
			notify(ChangedListener.FLOWVISOR, FFLOOD, floodPerm);
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	

	}
	
	@Override
	public void setFloodPerm(String floodPerm){
		setFloodPerm(1, floodPerm);
	}
	
	@Override
	public void mongoSetFloodPerm(String floodPerm) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(FLOODPERM, floodPerm));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
			notify(ChangedListener.FLOWVISOR, FFLOOD, floodPerm);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	
	@Override
	public void settrack_flows(Integer id, Boolean track_flows) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(STRACKID);
			ps.setBoolean(1, track_flows);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Track flows update had no effect.");
			notify(ChangedListener.FLOWVISOR, FTRACK, track_flows);
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	

	}

	@Override
	public void settrack_flows(Boolean track_flows) {
		settrack_flows(1, track_flows);

	}

	@Override
	public void mongoSetTrackFlows(Boolean track_flows) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(TRACK, track_flows));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
			notify(ChangedListener.FLOWVISOR, FTRACK, track_flows);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	
	@Override
	public void setstats_desc_hack(Integer id, Boolean stats_desc_hack) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SSTATSID);
			ps.setBoolean(1, stats_desc_hack);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Track flows update had no effect.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			notify(ChangedListener.FLOWVISOR, FSTATS, stats_desc_hack);
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}	

	}
	
	@Override
	public void setstats_desc_hack(Boolean stats_desc_hack) {
		setstats_desc_hack(1, stats_desc_hack);
	}
	
	@Override
	public void mongoSetStatsDescHack(Boolean stats_desc_hack) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(STATS, stats_desc_hack));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
			
			notify(ChangedListener.FLOWVISOR, FSTATS, stats_desc_hack);
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 	
	}
	
	public void setLogging(Integer id, String logging) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLOGGING);
			ps.setString(1, logging);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Track flows update had no effect.");
		} catch (SQLException e) {
			System.err.println(e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void setLogging(String logging) {
		setLogging(1, logging);
	}
	
	@Override
	public void mongoSetLogging(String logging) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(LOGGING, logging));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 	
	}

	@Override
	public void setLogFacility(Integer id, String logging) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLOGFACILITY);
			ps.setString(1, logging);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Unable to set the logging facility.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			
		} finally {
			close(set);
			close(ps);
			close(conn);	
		};
	}
	
	@Override
	public void setLogFacility(String logging) {
		setLogFacility(1, logging);
	}
	
	@Override
	public void mongoSetLogFacility(String logging) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(LOGFACILITY, logging));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void setLogIdent(Integer id, String logging) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLOGIDENT);
			ps.setString(1, logging);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Unable to set the logging facility.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			
		} finally {
			close(set);
			close(ps);
			close(conn);	
		};
	}
	
	@Override
	public void setLogIdent(String logging) {
		setLogIdent(1, logging);
	}
	
	@Override
	public void mongoSetLogIdent(String logging) {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(LOGIDENT, logging));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void setListenPort(Integer id, Integer port) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SLISTEN);
			ps.setInt(1, port);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Unable to set the logging facility.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void setListenPort(Integer port) throws ConfigError{
		setListenPort(1, port);
	}
	
	@Override
	public void mongoSetListenPort(Integer port) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(LISTEN, port));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
		
	@Override
	public void setAPIWSPort(Integer id, Integer port) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SAPIPORT);
			ps.setInt(1, port);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Unable to set the api port.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void setAPIWSPort(Integer port) throws ConfigError{
		setAPIWSPort(1, port);
	}
	
	@Override
	public void mongoSetAPIWSPort(Integer port) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(APIPORT, port));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
	}
	
	@Override
	public void setJettyPort(Integer id, Integer port) throws ConfigError {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(SJETTYPORT);
			ps.setInt(1, port);
			ps.setInt(2, id);
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Unable to set the jetty port.");
			} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
			
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
	}
	
	@Override
	public void setJettyPort(Integer port) throws ConfigError{
		setJettyPort(1, port);
	}
	
	@Override
	public void mongoSetJettyPort(Integer port) throws ConfigError {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorUpdate = new BasicDBObject("$set", new BasicDBObject(JETTYPORT, port));
			flowvisorColl.update(flowvisorQuery, flowvisorUpdate);
		
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
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
	
	public static Flowvisor getProxy() {
		return (Flowvisor) FVConfigurationController.instance()
		.getProxy(getInstance());
	}
	
	public static void addListener(FlowvisorChangedListener l) {
		FVConfigurationController.instance().addChangeListener(ChangedListener.FLOWVISOR, l);
	}
	
	public static void removeListener(FlowvisorChangedListener l) {
		FVConfigurationController.instance().removeChangeListener(ChangedListener.FLOWVISOR, l);
	}
	
	public  HashMap<String, Object> toJson(HashMap<String, Object> output) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		HashMap<String, Object> fv = new HashMap<String, Object>();
		LinkedList<Object> list = new LinkedList<Object>();
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(GALL);
			set = ps.executeQuery();
			while (set.next()) {		
				fv.put(APIPORT, set.getInt(APIPORT));
				fv.put(JETTYPORT, set.getInt(JETTYPORT));
				fv.put(CHECKPOINT, set.getBoolean(CHECKPOINT));
				fv.put(LISTEN, set.getInt(LISTEN));
				fv.put(TRACK, set.getBoolean(TRACK));
				fv.put(STATS, set.getBoolean(STATS));
				fv.put(TOPO, set.getBoolean(TOPO));
				fv.put(LOGGING, set.getString(LOGGING));
				fv.put(LOGIDENT, set.getString(LOGIDENT));
				fv.put(LOGFACILITY, set.getString(LOGFACILITY));
				fv.put(VERSION, set.getString(VERSION));
				fv.put(HOST, set.getString(HOST));
				fv.put(FLOODPERM, set.getString(FLOODPERM));
				fv.put(CONFIG, set.getString(CONFIG));
				fv.put(DB_VERSION, set.getInt(DB_VERSION));
				list.add(fv.clone());
				fv.clear();
			}
			output.put(FLOWVISOR, list);
			//System.out.println("list: " + list);
				
		} catch (SQLException e) {
			FVLog.log(LogLevel.WARN, null, "Failed to write Flowvisor base config : " + e.getMessage());
		} finally {
			close(set);
			close(ps);
			close(conn);
			
		}
		return output;
	}
	
	// Mongo change
	public  HashMap<String, Object> mongoToJson(HashMap<String, Object> output) {

		String mongo;
		String Flowvisor = "Flowvisor"; // This will go once we have consistency in the naming
		//String Slice = "Slice";
		DB conn = null;
		DBCollection flowvisorColl = null;
		LinkedList<Object> list = new LinkedList<Object>();
	
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
	
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject().append("_id", false).append("fscache", false);//.append(Slice, false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while(flowvisorSelect.hasNext()) {
					list.add(flowvisorSelect.next());
				}
			} finally {
				flowvisorSelect.close();
			}
			output.put(Flowvisor, list);	
			
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, "Failed to write mongo Flowvisor base config : " + e.getMessage());
		}
		return output;
	}
	
	@Override
	public void fromJson(ArrayList<HashMap<String, Object>> list) throws IOException {
		deleteAll();
		reset();
		for (HashMap<String, Object> row : list)
			insert(row);
	}
	
	private void deleteAll() {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(DELETE);
			ps.execute();
			ps = conn.prepareStatement(DELSWITCH);
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			close(ps);
			close(conn);
		}
	}

	private void insert(HashMap<String, Object> row) {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(DELETE);
			ps.execute();
			ps = conn.prepareStatement(INSERT);
			if (row.get(APIPORT) == null)
				row.put(APIPORT, new Double(8080));
			ps.setInt(1, ((Double) row.get(APIPORT)).intValue());
			
			if (row.get(JETTYPORT) == null)
				row.put(JETTYPORT, new Double(-1));
			ps.setInt(2, ((Double) row.get(JETTYPORT)).intValue());
			
			if (row.get(CHECKPOINT) == null)
				row.put(CHECKPOINT, false);
			ps.setBoolean(3, (Boolean) row.get(CHECKPOINT));
			
			if (row.get(LISTEN) == null)
				row.put(LISTEN, new Double(6633));
			ps.setInt(4, ((Double) row.get(LISTEN)).intValue());
			
			if (row.get(TRACK) == null)
				row.put(TRACK, false);
			ps.setBoolean(5, (Boolean) row.get(TRACK));
			
			if (row.get(STATS) == null)
				row.put(STATS, false);
			ps.setBoolean(6, (Boolean) row.get(STATS));
			
			if (row.get(TOPO) == null)
				row.put(TOPO, false);
			ps.setBoolean(7, (Boolean) row.get(TOPO));
			
			if (row.get(LOGGING) == null)
				row.put(LOGGING, "NOTE");
			ps.setString(8, (String) row.get(LOGGING));
			
			if (row.get(LOGIDENT) == null)
				row.put(LOGIDENT, "flowvisor");
			ps.setString(9, (String) row.get(LOGIDENT));
			
			if (row.get(LOGFACILITY) == null)
				row.put(LOGFACILITY, "LOG_LOCAL7");
			ps.setString(10, (String) row.get(LOGFACILITY));
			
			//if (row.get(VERSION) == null)
			//row.put(VERSION, FlowVisor.FLOWVISOR_VERSION);
			ps.setString(11, FlowVisor.FLOWVISOR_VERSION);
			
			if (row.get(HOST) == null)
				row.put(HOST, "localhost");
			ps.setString(12, (String) row.get(HOST));
			
			if (row.get(FLOODPERM) == null)
				row.put(FLOODPERM, "fvadmin");
			ps.setString(13, (String) row.get(FLOODPERM));
			
			if (row.get(CONFIG) == null)
				row.put(CONFIG, "default");
			ps.setString(14, (String) row.get(CONFIG));
			
			if (row.get(DB_VERSION) == null)
				row.put(DB_VERSION, new Double(FlowVisor.FLOWVISOR_DB_VERSION));
			ps.setInt(15, ((Double) row.get(DB_VERSION)).intValue()); 
			
			if (ps.executeUpdate() == 0)
				FVLog.log(LogLevel.WARN, null, "Insertion failed... siliently.");
			} catch (SQLException e) {
				e.printStackTrace();
		} finally {
			close(set);
			close(ps);
			close(conn);	
		}
		
	}
	
	private void reset() {
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(RESETFLOWVISOR);
			ps.execute();
			ps = conn.prepareStatement(RESETSWITCH);
			ps.execute();
		} catch (SQLException e) {
			System.err.println("Reseting index on table flowvisor failed : " + e.getMessage());
		} finally {
			close(ps);
			close(conn);
		}
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void mongoFromJson(ArrayList<HashMap<String, Object>> list) throws IOException {
		
		String mongo;
		String Flowvisor = "Flowvisor"; // This will go once we have consistency in the naming
		//String Slice = "Slice";
		String Switch = "Switch";
		String FlowSpaceRule = "FlowSpaceRule";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
						
			if (list.size() == 0) {
				
				LoadConfig.defaultMongoConfig("");
			} else {
				
				flowvisorColl = conn.getCollection(Flowvisor);
				flowvisorColl.drop();
				flowvisorColl = conn.getCollection(Flowvisor);
				
				ArrayList<DBObject> flowvisorList = new ArrayList<DBObject>();
				
				//System.out.println(list.size());
					
				for (HashMap<String, Object> row : list) {
					
					BasicDBObject flowvisorSet = new BasicDBObject();
					
					if (row.get(APIPORT) == null)
						row.put(APIPORT, new Double(8080));
					flowvisorSet.put(APIPORT, ((Double) row.get(APIPORT)).intValue());
					
					if (row.get(DB_VERSION) == null)
						row.put(DB_VERSION, new Double(FlowVisor.FLOWVISOR_DB_VERSION));
					flowvisorSet.put(DB_VERSION, ((Double) row.get(DB_VERSION)).intValue());
					
					if (row.get(HOST) == null)
						row.put(HOST, "localhost");
					flowvisorSet.put(HOST, (String) row.get(HOST));
					
					if (row.get(LOGIDENT) == null)
						row.put(LOGIDENT, "flowvisor");
					flowvisorSet.put(LOGIDENT, (String) row.get(LOGIDENT));
				
					if (row.get(CHECKPOINT) == null)
						row.put(CHECKPOINT, false);
					flowvisorSet.put(CHECKPOINT, (Boolean) row.get(CHECKPOINT));

					if (row.get(LISTEN) == null)
						row.put(LISTEN, new Double(6633));
					flowvisorSet.put(LISTEN, ((Double) row.get(LISTEN)).intValue());
					
					if (row.get(LOGGING) == null)
						row.put(LOGGING, "NOTE");
					flowvisorSet.put(LOGGING, (String) row.get(LOGGING));
					
					if (row.get(TOPO) == null)
						row.put(TOPO, false);
					flowvisorSet.put(TOPO, (Boolean) row.get(TOPO));
					
					if (row.get(LOGFACILITY) == null)
						row.put(LOGFACILITY, "LOG_LOCAL7");
					flowvisorSet.put(LOGFACILITY, (String) row.get(LOGFACILITY));
					
					if (row.get(VERSION) == null)
						row.put(VERSION, FlowVisor.FLOWVISOR_VERSION);
					flowvisorSet.put(VERSION, (String) row.get(VERSION));
					
					if (row.get(CONFIG) == null)
						row.put(CONFIG, "default");
					flowvisorSet.put(CONFIG, (String) row.get(CONFIG));
					
					if (row.get(JETTYPORT) == null)
						row.put(JETTYPORT, new Double(8081));
					flowvisorSet.put(JETTYPORT, ((Double) row.get(JETTYPORT)).intValue());
			
					if (row.get(FLOODPERM) == null)
						row.put(FLOODPERM, "fvadmin");
					flowvisorSet.put(FLOODPERM, (String) row.get(FLOODPERM));	
					
					if (row.get(TRACK) == null)
						row.put(TRACK, false);
					flowvisorSet.put(TRACK, (Boolean) row.get(TRACK));
				
					if (row.get(STATS) == null)
						row.put(STATS, false);
					flowvisorSet.put(STATS, (Boolean) row.get(STATS));
					
					if (row.get(FSCACHE) == null) 
						row.put(FSCACHE, new Double(30));
					flowvisorSet.put(FSCACHE, ((Double) row.get(FSCACHE)).intValue());
					
					if (row.get("Slice") == null) {
						
						ArrayList<BasicDBObject> sliceList = LoadConfig.defaultSlice();
						flowvisorSet.put("Slice", sliceList);
					} else {
						
						ArrayList<HashMap<String, Object>> iterList = (ArrayList<HashMap<String, Object>>) row.get("Slice");
						ArrayList<BasicDBObject> sliceList = new ArrayList<BasicDBObject>();
						
						for (HashMap<String, Object> sliceRow : iterList) {
							
							BasicDBObject sliceSet = new BasicDBObject();
							
							sliceSet.put(Slice.EMAIL, (String) sliceRow.get(Slice.EMAIL));
							
							if (sliceRow.get(Slice.ADMINDOWN) == null)
								sliceRow.put(Slice.ADMINDOWN, true);
							sliceSet.put(Slice.ADMINDOWN, (Boolean) sliceRow.get(Slice.ADMINDOWN));
						
							sliceSet.put(Slice.CREATOR, (String) sliceRow.get(Slice.CREATOR));
							sliceSet.put(Slice.SALT, (String) sliceRow.get(Slice.SALT));
							
							if (sliceRow.get(Slice.DROP) == null)
								sliceRow.put(Slice.DROP, "exact");
							sliceSet.put(Slice.DROP, (String) sliceRow.get(Slice.DROP));
							
							if (sliceRow.get(Slice.FMLIMIT) == null)
								sliceRow.put(Slice.FMLIMIT, new Double(-1));
							sliceSet.put(Slice.FMLIMIT, ((Double) sliceRow.get(Slice.FMLIMIT)).intValue());
			
							sliceSet.put(Slice.SLICE, (String) sliceRow.get(Slice.SLICE));
							sliceSet.put(Slice.PORT, ((Double) sliceRow.get(Slice.PORT)).intValue());
							sliceSet.put(Slice.HOST, (String) sliceRow.get(Slice.HOST));
							sliceSet.put(Slice.FMTYPE, (String) sliceRow.get(Slice.FMTYPE));
							sliceSet.put(Slice.CRYPT, (String) sliceRow.get(Slice.CRYPT));
							
							if (sliceRow.get(Slice.LLDP) == null) 
								sliceRow.put(Slice.LLDP, true);
							sliceSet.put(Slice.LLDP, (Boolean)sliceRow.get(Slice.LLDP));
							///*
							if (sliceRow.get(FlowSpaceRule) == null) {
								
								ArrayList<BasicDBObject> FSRList = new ArrayList<BasicDBObject>();
								sliceSet.put(FlowSpaceRule, FSRList);
							} else {
		
								sliceSet.put(FlowSpaceRule, (ArrayList<BasicDBObject>)sliceRow.get(FlowSpaceRule));
								// This will be fun
							}
							//*/
							
							sliceList.add(sliceSet);
						}
						
						flowvisorSet.put("Slice", sliceList);
					}
					///*
					if (row.get("Switch") == null) {
						
						ArrayList<BasicDBObject> switchList = new ArrayList<BasicDBObject>();
						flowvisorSet.put("Switch", switchList);
					} else {
						
						flowvisorSet.put("Switch", (ArrayList<BasicDBObject>)row.get(Switch));
						// This also will be fun
					}
					
					flowvisorList.add(flowvisorSet);
					//*/
				}
				
				flowvisorColl.insert(flowvisorList);
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
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
		FVLog.log(LogLevel.INFO, null, "Updating FlowVisor database table.");
		if (version == 0) {
			processAlter("ALTER TABLE Flowvisor ADD COLUMN " + DB_VERSION + " INT");
			version++;
		}
		if (version == 1) {
			processAlter("ALTER TABLE Flowvisor ADD COLUMN " + FSCACHE + " INT DEFAULT 30");
			//processAlter("ALTER TABLE Flowvisor DROP COLUMN " + APIPORT );
			processAlter("ALTER TABLE Flowvisor DROP COLUMN " + JETTYPORT );
			//processAlter("ALTER TABLE Flowvisor ADD COLUMN " + APIPORT + " INT DEFAULT 8081");
			processAlter("ALTER TABLE Flowvisor ADD COLUMN " + JETTYPORT + " INT DEFAULT 8081");
			
			
			
			version++;
		}
		processAlter("UPDATE FlowVisor SET " + DB_VERSION + " = " + FlowVisor.FLOWVISOR_DB_VERSION);
		
	}

	@Override
	public int fetchDBVersion() {
		String check = "SELECT * FROM FLOWVISOR";
		String version = "SELECT " + DB_VERSION + " FROM Flowvisor";
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet set = null;
		try {
			conn = settings.getConnection();
			ps = conn.prepareStatement(check);
			set = ps.executeQuery();
			try {
				set.findColumn(DB_VERSION);
			} catch (SQLException e) {
				return 0;
			}
			ps = conn.prepareStatement(version);
			set = ps.executeQuery();
			if (set.next()) 
				return set.getInt(DB_VERSION);
			else {
				System.err.println("Database empty, assuming latest DB Version.");
				return FlowVisor.FLOWVISOR_DB_VERSION;
				/*
				System.exit(1);*/
			}
				
		} catch (SQLException e) {
			if (e.getNextException() != null)
				System.err.println("Embedded DB issue, exiting : " + e.getNextException().getMessage());
			else
				System.err.println("Embedded DB missing, exiting: " + e.getMessage());
			System.exit(1);
		} finally {
			close(ps);
			close(conn);
		}
		return 0;
	}

	@Override
	public int mongoFetchDBVersion() {
		
		String mongo;
		String Flowvisor = "Flowvisor";
		DB conn = null;
		DBCollection flowvisorColl = null;
		
		try {
			conn = settings.getMongoConnection();
			flowvisorColl = conn.getCollection(Flowvisor);
			
			BasicDBObject flowvisorQuery = new BasicDBObject();
			BasicDBObject flowvisorFields = new BasicDBObject(DB_VERSION, 1).append("_id", false);
			DBCursor flowvisorSelect = flowvisorColl.find(flowvisorQuery, flowvisorFields);
			
			try {
				while (flowvisorSelect.hasNext()) {
					Integer queryResult = (Integer) flowvisorSelect.next().get(DB_VERSION);
					if (queryResult != null)
						return queryResult;
					else {
						System.err.println("Database empty, assuming latest DB Version!");
						return FlowVisor.FLOWVISOR_DB_VERSION;
					}
						
				}
			} finally {
				flowvisorSelect.close();
			}
		} catch (UnknownHostException e) {
			FVLog.log(LogLevel.WARN, null, e.getMessage());
		} 
		return 0;
	}

}
