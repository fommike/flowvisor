package org.flowvisor.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import net.minidev.json.JSONObject;

import org.flowvisor.FlowVisor;
import org.flowvisor.api.APIAuth;
import org.flowvisor.api.handlers.HandlerUtils;
import org.flowvisor.exceptions.DuplicateControllerException;
import org.flowvisor.flows.FlowMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

/**
 * List of things to populate FVConfig with on startup Everything here can be
 * overridden from the config file. 
 * 
 * If called with a parameter, it dumps the config to that file.
 *
 * @author ash
 *
 */
@SuppressWarnings("deprecation")
public class LoadConfig {
	
	//private static int WILDCARDS = OFMatch.OFPFW_ALL & ~(OFMatch.OFPFW_DL_SRC | OFMatch.OFPFW_IN_PORT);
	
	private static String CLEAR = "DELETE FROM jFSRSlice;\n" +
			"ALTER TABLE jFSRSlice ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM Slice;\n" +
			"ALTER TABLE Slice ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM FlowSpaceRule;\n" +
			"ALTER TABLE FlowSpaceRule ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM Flowvisor;\n" +
			"ALTER TABLE Flowvisor ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM Switch; \n" +
			"ALTER TABLE Switch ALTER COLUMN id RESTART WITH 1;\n";
			
	private static String  defaultconfig = "DELETE FROM jFSRSlice;\n" +
			"ALTER TABLE jFSRSlice ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM Slice;\n" +
			"ALTER TABLE Slice ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM FlowSpaceRule;\n" +
			"ALTER TABLE FlowSpaceRule ALTER COLUMN id RESTART WITH 1;\n" +
			"DELETE FROM Flowvisor;\n" +
			"ALTER TABLE Flowvisor ALTER COLUMN id RESTART WITH 1;\n" +
			"INSERT INTO Flowvisor(config_name,run_topology_server,db_version, version) VALUES('default', false, " 
				+ FlowVisor.FLOWVISOR_DB_VERSION + ",'" + FlowVisor.FLOWVISOR_VERSION + "');\n" +
			"INSERT INTO Slice(flowvisor_id, flowmap_type, name, creator, passwd_crypt, passwd_salt, " +
				"controller_hostname, controller_port, contact_email) VALUES(1, 1, 'fvadmin', 'fvadmin', " +
				"'CHANGEME', 'CHANGESALT', 'none', 0, 'fvadmin@localhost');\n" ;
	
	
	public static void defaultConfig(String passwd) {
		String salt = APIAuth.getSalt();
		String pass = APIAuth.makeCrypt(salt, passwd);
		String config = defaultconfig.replace("CHANGEME", pass);
		config = config.replace("CHANGESALT", salt);
		ConfDBHandler db = new ConfDBHandler();
		try {
			importSQL(db.getConnection(), new StringBufferInputStream(config));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	// Mongo change
	public static void defaultMongoConfig(String passwd) {
		
		String mongo;
		
		ConfDBHandler db = new ConfDBHandler();
		
		DB flowvisorDB = null;
		DBCollection flowvisorColl = null;
	
		String Switch = "Switch";
		String Flowvisor = "Flowvisor";
		String Slice = "Slice";
		
		try {
			flowvisorDB = db.getMongoConnection();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		flowvisorColl = flowvisorDB.getCollection(Flowvisor);

		if (flowvisorColl.getCount() != 0) {
			
			flowvisorColl.drop();
			flowvisorColl = flowvisorDB.getCollection(Flowvisor);
		}
		
		ArrayList<DBObject> flowvisorList = new ArrayList<DBObject>();
		BasicDBObject flowvisorSet = defaultFlowvisor();
	
		ArrayList<BasicDBObject> sliceList = defaultSlice();
		flowvisorSet.put(Slice, sliceList);
		
		ArrayList<BasicDBObject> switchList = new ArrayList<BasicDBObject>(); //defaultSwitch(); 
		flowvisorSet.put(Switch, switchList);
		
		flowvisorList.add(flowvisorSet);
		flowvisorColl.insert(flowvisorList);
		
		/*
		System.out.println("Select * FROM Flowvisor: ");
		DBCursor flowvisorSelect = flowvisorColl.find();
		
		try {
			while (flowvisorSelect.hasNext()) {
				System.out.println(flowvisorSelect.next());
			}
		} finally {
			flowvisorSelect.close();
		}
		*/
	}
	
	// Mongo change
	public static BasicDBObject defaultFlowvisor() {
		
		String mongo;
		BasicDBObject flowvisorSet = new BasicDBObject();
		
		flowvisorSet.put("api_webserver_port", 8080);
		flowvisorSet.put("db_version", FlowVisor.FLOWVISOR_DB_VERSION);
		flowvisorSet.put("host", "localhost");
		flowvisorSet.put("log_ident", "flowvisor");
		flowvisorSet.put("checkpointing", false);
		flowvisorSet.put("listen_port",  6633);
		flowvisorSet.put("logging", "NOTE");
		flowvisorSet.put("run_topology_server", false);
		flowvisorSet.put("log_facility", "LOG_LOCAL7");
		flowvisorSet.put("version", FlowVisor.FLOWVISOR_VERSION);
		flowvisorSet.put("config_name", "default");
		flowvisorSet.put("api_jetty_webserver_port", 8081);
		flowvisorSet.put("default_flood_perm", "fvadmin");	
		flowvisorSet.put("track_flows", false);
		flowvisorSet.put("stats_desc_hack", false);		
		flowvisorSet.put("fscache", 30);
		
		return flowvisorSet;
	}
	
	// Mongo change
	public static ArrayList<BasicDBObject> defaultSlice() {
	
		String mongo;
		String FlowSpaceRule = "FlowSpaceRule";
		String salt = APIAuth.getSalt();
		String pass = APIAuth.makeCrypt(salt, "");
		
		ArrayList<BasicDBObject> sliceList = new ArrayList<BasicDBObject>();
		BasicDBObject sliceSet = new BasicDBObject();
		
		sliceSet.put("contact_email", "fvadmin@localhost");
		sliceSet.put("admin_status", true);
		sliceSet.put("creator", "fvadmin");
		sliceSet.put("passwd_salt", salt);
		sliceSet.put("drop_policy", "exact");
		sliceSet.put("max_flow_rules", -1);
		sliceSet.put("name", "fvadmin");
		sliceSet.put("controller_port", 0);
		sliceSet.put("controller_hostname", "none");
		sliceSet.put("flowmap_type",  FlowMap.type.values()[1].getText()); 	
		sliceSet.put("passwd_crypt", pass);
		sliceSet.put("lldp_spam", true);
	
		//ArrayList<BasicDBObject> FSRList = new ArrayList<BasicDBObject>();
		/*
		BasicDBObject FSR11 = new BasicDBObject();
		FSR11.put("priority", 1);
		FSR11.put("in_port", 2);
		FSRList.add(FSR11);
		
		BasicDBObject FSR12 = new BasicDBObject();
		FSR12.put("priority", 1);
		FSR12.put("in_port", 3);
		FSRList.add(FSR12);
		*/
		//sliceSet.put(FlowSpaceRule, FSRList);
		
		BasicDBObject sliceSet1 = new BasicDBObject();
		
		sliceSet1.put("contact_email", "upper@localhost");
		sliceSet1.put("admin_status", false);
		sliceSet1.put("creator", "fvadmin_11");
		sliceSet1.put("passwd_salt", salt);
		sliceSet1.put("drop_policy", "exact");
		sliceSet1.put("max_flow_rules", -1);
		sliceSet1.put("name", "upper");
		sliceSet1.put("controller_port", 6640);
		sliceSet1.put("controller_hostname", "127.0.0.1");
		sliceSet1.put("flowmap_type", FlowMap.type.values()[1].getText());	
		sliceSet1.put("passwd_crypt", pass);
		sliceSet1.put("lldp_spam", false);
		
		/*
		ArrayList<BasicDBObject> FSRList1 = new ArrayList<BasicDBObject>();
		
		BasicDBObject FSR21 = new BasicDBObject();
		FSR21.put("priority", 1);
		FSR21.put("in_port", 2);
		FSRList1.add(FSR21);
		
		BasicDBObject FSR22 = new BasicDBObject();
		FSR22.put("priority", 3);
		FSR22.put("in_port", 4);
		FSRList1.add(FSR22);
		
		sliceSet1.put(FlowSpaceRule, FSRList1);
		*/
		
		sliceList.add(sliceSet);
		//sliceList.add(sliceSet1);
		
		return sliceList;
	}
	
	// Mongo change
	public static ArrayList<BasicDBObject> defaultSwitch() {
		
		String mongo;
		ArrayList<BasicDBObject> switchList = new ArrayList<BasicDBObject>();
		
		BasicDBObject switchSet = new BasicDBObject();
		switchSet.put("dpid", 1L);
		switchSet.put("flood_perm", "");
		//BasicDBObject sliceLimitSet = new BasicDBObject();
		//sliceLimitSet.put(Switch.FMLIMIT, -1);
		//sliceLimitSet.put(Switch.RATELIMIT, -1);
		//BasicDBObject sliceLimit = new BasicDBObject("fvadmin", sliceLimitSet);
		//switchSet.put(Switch.LIMITS, sliceLimit);
		
		BasicDBObject switchSet1 = new BasicDBObject();
		switchSet1.put("dpid", 2L);
		switchSet1.put("flood_perm", "");
		BasicDBObject sliceLimitSet1 = new BasicDBObject();
		sliceLimitSet1.put(Switch.FMLIMIT, -1);
		sliceLimitSet1.put(Switch.RATELIMIT, -1);
		BasicDBObject sliceLimit1 = new BasicDBObject("fvadmin", sliceLimitSet1);
		switchSet1.put(Switch.LIMITS, sliceLimit1);
		/*
		BasicDBObject switchSet2 = new BasicDBObject();
		switchSet2.put("dpid", 3L);
		switchSet2.put("flood_perm", "upper");
		BasicDBObject sliceLimitSet2 = new BasicDBObject();
		sliceLimitSet2.put(Switch.FMLIMIT, -1);
		sliceLimitSet2.put(Switch.RATELIMIT, -1);
		BasicDBObject sliceLimit2 = new BasicDBObject("fvadmin", sliceLimitSet2);
		switchSet2.put(Switch.LIMITS, sliceLimit2);
		
		BasicDBObject switchSet3 = new BasicDBObject();
		switchSet3.put("dpid", 4L);
		switchSet3.put("flood_perm", "lower");
		BasicDBObject sliceLimitSet3 = new BasicDBObject();
		sliceLimitSet3.put(Switch.FMLIMIT, -1);
		sliceLimitSet3.put(Switch.RATELIMIT, -1);
		BasicDBObject sliceLimit3 = new BasicDBObject("fvadmin", sliceLimitSet3);
		switchSet3.put(Switch.LIMITS, sliceLimit3);
		*/
		switchList.add(switchSet);
		//switchList.add(switchSet1);
		//switchList.add(switchSet2);
		//switchList.add(switchSet3);
		
		return switchList;
	}
	
	public static void loadConfig(String filename) {
		ConfDBHandler db = new ConfDBHandler();
		try {
			clearDB(db);
			importSQL(db.getConnection(), new FileInputStream(filename));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void clearDB(ConfDBHandler db) {
		try {
			importSQL(db.getConnection(), new StringBufferInputStream(CLEAR));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	private static void importSQL(Connection conn, InputStream in) throws SQLException
	{
	        Scanner s = new Scanner(in);
	        s.useDelimiter("(;(\r)?\n)|(--\n)");
	        Statement st = null;
	        conn.setAutoCommit(true);
	        
	        try
	        {
	                st = conn.createStatement();
	                while (s.hasNext())
	                {
	                        String line = s.next();
	                        if (line.startsWith("/*!") && line.endsWith("*/"))
	                        {
	                                int i = line.indexOf(' ');
	                                line = line.substring(i + 1, line.length() - " */".length());
	                        }

	                        if (line.trim().length() > 0)
	                        {
	                                st.execute(line);
	                        }
	                }
	        }
	        finally
	        {
	                if (st != null) st.close();
	                conn.close();
	        }
	}
	
	/**
	 * Print default config to stdout
	 *
	 * @param args
	 * @throws FileNotFoundException
	 * @throws SQLException 
	 */

	public static void main(String args[]) throws FileNotFoundException, ConfigError {
		if (args.length > 0) {
			System.out.println("We are loading new config!");
			FVConfigurationController.init(new ConfDBHandler());
			//FVConfig.readFromFile(args[0]);
			//FVConfig.mongoReadFromFile(args[0]);

			/*
			List<String> slices =  FVConfig.mongoGetAllSlices();
			//System.out.println(slices);
			for (String slice: slices) {
				if (!slice.equals("fvadmin")) {
					FlowvisorImpl.getProxy().mongoSetFloodPerm(slice + "11");
				}
			}
			*/
			
			//SwitchImpl.getProxy().mongoSetFloodPerm(20L, "lower");
			//SwitchImpl.getProxy().mongoSetMaxFlowMods("upper",1L, 44);
			
			/*
			try {
				SliceImpl.getProxy().mongoCreateSlice("mike", "localhost", 3330, 
						"exact", "123", "321232", "mike@admin.com", "fvadmin", 1);
			} catch (InvalidSliceName e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (DuplicateControllerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/
			
			System.out.println(FlowvisorImpl.getProxy().mongoFetchDBVersion());
			FVConfigurationController.instance().shutdown();
			return;
		}
			
		System.err.println("Generating default config");
		LoadConfig.defaultConfig("CHANGEME");
		System.err.println("Done."); 
	}
}
