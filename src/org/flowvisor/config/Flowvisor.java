package org.flowvisor.config;

/**
 * A proxy interface for flowvisor specific 
 * information
 * 
 * 
 * @author ash
 *
 */
public interface Flowvisor extends FVAppConfig {
	
	// COLUMN NAMES
	public static String TRACK = "track_flows";
	public static String STATS = "stats_desc_hack";
	public static String LISTEN = "listen_port";
	public static String APIPORT = "api_webserver_port";
	public static String CHECKPOINT = "checkpointing";
	public static String JETTYPORT = "api_jetty_webserver_port";
	public static String FLOODPERM = "default_flood_perm";
	public static String LOGIDENT = "log_ident";
	public static String LOGGING = "logging";
	public static String LOGFACILITY = "log_facility";
	public static String TOPO = "run_topology_server";
	public static String VERSION = "version";
	public static String HOST = "host";
	public static String CONFIG = "config_name";
	public static String DB_VERSION = "db_version";
	public static String FSCACHE = "fscache";
	
	// Table name
	public static String FLOWVISOR = "flowvisor";
	
	public Boolean gettrack_flows(Integer id) throws ConfigError;
	public Boolean gettrack_flows() throws ConfigError;
	// Mongo change
	//public Boolean mongoGetTrackFlows(Integer id) throws ConfigError;
	public Boolean mongoGetTrackFlows() throws ConfigError;
	
	public Boolean getstats_desc_hack(Integer id);
	public Boolean getstats_desc_hack();
	// Mongo change
	//public Boolean mongoGetStatsDescHack(Integer id) throws ConfigError;
	public Boolean mongoGetStatsDescHack() throws ConfigError;
	
	public Integer getAPIWSPort(Integer id) throws ConfigError;
	public Integer getAPIWSPort() throws ConfigError;
	// Mongo change
	//public Integer mongoGetAPIWSPort(Integer id) throws ConfigError;
	public Integer mongoGetAPIWSPort() throws ConfigError;
	
	public Integer getJettyPort(Integer id) throws ConfigError;
	public Integer getJettyPort() throws ConfigError;
	// Mongo change
	//public Integer mongoGetJettyPort(Integer id) throws ConfigError;
	public Integer mongoGetJettyPort() throws ConfigError;
	
	public Integer getListenPort(Integer id) throws ConfigError;
	public Integer getListenPort() throws ConfigError;
	// Mongo change
	//public Integer mongoGetListenPort(Integer id) throws ConfigError;
	public Integer mongoGetListenPort() throws ConfigError;
	
	public Boolean getCheckPoint(Integer id) throws ConfigError;
	public Boolean getCheckPoint() throws ConfigError;
	// Mongo change
	//public Boolean mongoGetCheckPoint(Integer id) throws ConfigError;
	public Boolean mongoGetCheckPoint() throws ConfigError;
	
	public String getFloodPerm(Integer id) throws ConfigError;
	public String getFloodPerm() throws ConfigError;
	// Mongo change
	//public String mongoGetFloodPerm(Integer id) throws ConfigError;
	public String mongoGetFloodPerm() throws ConfigError;
	
	public String getLogIdent(Integer id) throws ConfigError;
	public String getLogIdent() throws ConfigError;
	// Mongo change
	//public String mongoGetLogIdent(Integer id) throws ConfigError;
	public String mongoGetLogIdent() throws ConfigError;
	
	public String getLogging(Integer id) throws ConfigError;
	public String getLogging() throws ConfigError;
	// Mongo change
	//public String mongoGetLogging(Integer id) throws ConfigError;
	public String mongoGetLogging() throws ConfigError;
	
	public String getLogFacility(Integer id) throws ConfigError;
	public String getLogFacility() throws ConfigError;
	// Mongo change
	//public String mongoGetLogFacility(Integer id) throws ConfigError;
	public String mongoGetLogFacility() throws ConfigError;
	
	public Boolean getTopologyServer(int id) throws ConfigError;
	public Boolean getTopologyServer() throws ConfigError;
	// Mongo change
	//public Boolean mongoGetTopologyServer(int id) throws ConfigError;
	public Boolean mongoGetTopologyServer() throws ConfigError;
	
	public Integer getFlowStatsCache() throws ConfigError;
	// Mongo change
	public Integer mongoGetFlowStatsCache() throws ConfigError;
	
	public void settrack_flows(Integer id, Boolean track_flows);
	public void settrack_flows(Boolean track_flows);
	// Mongo change
	//public void mongoSetTrackFlows(Integer id, Boolean track_flows);
	public void mongoSetTrackFlows(Boolean track_flows);
	
	public void setstats_desc_hack(Integer id, Boolean stats_desc_hack);
	public void setstats_desc_hack(Boolean stats_desc_hack);
	// Mongo change
	//public void mongoSetStatsDescHack(Integer id, Boolean stats_desc_hack);
	public void mongoSetStatsDescHack(Boolean stats_desc_hack);
	
	public void setFloodPerm(Integer id, String floodPerm);
	public void setFloodPerm(String floodPerm);
	// Mongo change
	//public void mongoSetFloodPerm(Integer id, String floodPerm);
	public void mongoSetFloodPerm(String floodPerm);
	
	public void setLogging(Integer id, String logging);
	public void setLogging(String logging);
	// Mongo change
	//public void mongoSetLogging(Integer id, String logging);
	public void mongoSetLogging(String logging);
	
	public void setLogFacility(Integer id, String logging);
	public void setLogFacility(String logging);
	// Mongo change
	//public void mongoSetLogFacility(Integer id, String logging);
	public void mongoSetLogFacility(String logging);
	
	public void setLogIdent(Integer id, String logging);
	public void setLogIdent(String logging);
	// Mongo change
	//public void mongoSetLogIdent(Integer id, String logging);
	public void mongoSetLogIdent(String logging);
	
	public void setTopologyServer(Integer id, Boolean topo) throws ConfigError;
	public void setTopologyServer(Boolean topo) throws ConfigError;
	// Mongo change
	//public void mongoSetTopologyServer(Integer id, Boolean topo) throws ConfigError;
	public void mongoSetTopologyServer(Boolean topo) throws ConfigError;
	
	public void setListenPort(Integer id, Integer port) throws ConfigError;
	public void setListenPort(Integer port) throws ConfigError;
	// Mongo change
	//public void mongoSetListenPort(Integer id, Integer port) throws ConfigError;
	public void mongoSetListenPort(Integer port) throws ConfigError;
	
	public void setAPIWSPort(Integer id, Integer port) throws ConfigError;
	public void setAPIWSPort(Integer port) throws ConfigError;
	// Mongo change
	//public void mongoSetAPIWSPort(Integer id, Integer port) throws ConfigError;
	public void mongoSetAPIWSPort(Integer port) throws ConfigError;
	
	public void setJettyPort(Integer id, Integer port) throws ConfigError;
	public void setJettyPort(Integer port) throws ConfigError;
	// Mongo change
	//public void mongoSetJettyPort(Integer id, Integer port) throws ConfigError;
	public void mongoSetJettyPort(Integer port) throws ConfigError;
	
	public void setFlowStatsCache(Integer timer) throws ConfigError;
	// Mongo change
	public void mongoSetFlowStatsCache(Integer timer) throws ConfigError;
	
	public int fetchDBVersion();
	// Mongo change
	public int mongoFetchDBVersion();
	
}
