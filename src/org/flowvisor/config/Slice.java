package org.flowvisor.config;


import java.util.LinkedList;

import org.flowvisor.exceptions.DuplicateControllerException;

public interface Slice extends FVAppConfig {
	

	// COLUMN NAMES
	public static String LLDP = "lldp_spam";
	public static String DROP = "drop_policy";
	public static String HOST = "controller_hostname";
	public static String PORT = "controller_port";
	public static String SLICE = "name";
	public static String CREATOR = "creator";
	public static String EMAIL = "contact_email";
	public static String CRYPT = "passwd_crypt";
	public static String SALT = "passwd_salt";
	public static String FMTYPE = "flowmap_type";
	public static String FLOWVISORID = "flowvisor_id";
	public static String FMLIMIT = "max_flow_rules";
	public static String ADMINDOWN = "admin_status";
	
	// Table name
	public static String TSLICE = "Slice";

	public void setlldp_spam(String sliceName, Boolean LLDPSpam);
	// Mongo change
	public void mongoSetLLDPSpam(String sliceName, Boolean LLDPSpam);
	
	public void setdrop_policy(String sliceName, String policy);
	// Mongo
	public void mongoSetDropPolicy(String sliceName, String policy);
	
	public void setcontroller_hostname(String sliceName, String name) throws ConfigError;
	// Mongo change
	public void mongoSetControllerHostname(String sliceName, String name) throws ConfigError;
	
	public void setcontroller_port(String sliceName, Integer port) throws ConfigError;
	// Mongo change
	public void mongoSetControllerPort(String sliceName, Integer port) throws ConfigError;
	
	public void setContactEmail(String sliceName, String email) throws ConfigError;
	// Mongo change
	public void mongoSetContactEmail(String sliceName, String email) throws ConfigError;
	
	public void setPasswd(String sliceName, String salt, String crypt) throws ConfigError;
	// Mongo change
	public void mongoSetPasswd(String sliceName, String salt, String crypt) throws ConfigError;
	
	public void setMaxFlowMods(String sliceName, int limit) throws ConfigError;
	// Mongo change 
	public void mongoSetMaxFlowMods(String sliceName, int limit) throws ConfigError;
	
	public Boolean getlldp_spam(String sliceName);
	// Mongo change
	public Boolean mongoGetLLDPSpam(String sliceName);
	
	public String getdrop_policy(String sliceName);
	//Mongo change
	public String mongoGetDropPolicy(String sliceName);
	
	public String getcontroller_hostname(String sliceName) throws ConfigError;
	// Mongo change
	public String mongoGetControllerHostname(String sliceName) throws ConfigError;
	
	public Integer getcontroller_port(String sliceName) throws ConfigError;
	// Mongo change
	public Integer mongoGetControllerPort(String sliceName) throws ConfigError;
	
	/** Mongo **/
	public String getPasswdElm(String sliceName, String elm) throws ConfigError;
	public String getCreator(String sliceName) throws ConfigError;
	// Mongo change
	public String mongoGetCreator(String sliceName) throws ConfigError;
	
	public String getEmail(String sliceName) throws ConfigError;
	// Mongo change
	public String mongoGetEmail(String sliceName) throws ConfigError;
	
	public LinkedList<String> getAllSliceNames() throws ConfigError;
	// Mongo change
	public LinkedList<String> mongoGetAllSliceNames() throws ConfigError;
		
	public Integer getMaxFlowMods(String sliceName) throws ConfigError;
	// Mongo change
	public Integer mongoGetMaxFlowMods(String sliceName) throws ConfigError;
	
	public Boolean checkSliceName(String sliceName);
	//Mongo change
	public Boolean mongoCheckSliceName(String sliceName);
	
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int flowvisor_id, int type) throws InvalidSliceName,
			DuplicateControllerException;
	
	// Mongo change
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int type) 
					throws InvalidSliceName,DuplicateControllerException;
	
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice, int flowvisor_id)
					throws InvalidSliceName, DuplicateControllerException;
	
	// Mongo change
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice) throws InvalidSliceName, DuplicateControllerException;
	
	public void createSlice(String sliceName,String controllerHostname, 
			int controllerPort, String dropPolicy, String passwd,
			String sliceEmail, String creatorSlice) 
					throws InvalidSliceName, DuplicateControllerException;
	
	public void createSlice(String sliceName,
			String controllerHostname, int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice)
					throws InvalidSliceName, DuplicateControllerException;
	
	// Mongo change
	public void mongoCreateSlice(String sliceName, String controllerHostname, 
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice)
					throws InvalidSliceName, DuplicateControllerException;
	
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice,
			int flowvisor_id, int type) 
					throws InvalidSliceName, DuplicateControllerException;
	
	// Mongo change
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, int type) 
					throws InvalidSliceName, DuplicateControllerException;
	
	public void deleteSlice(String sliceName) throws InvalidSliceName;
	// Mongo change
	public void mongoDeleteSlice(String sliceName) throws InvalidSliceName;
		
	
	// ADDED FOR JSONRPC 
	public void createSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, boolean LLDPSpam, 
			int maxFlowMods, int flowvisor_id, int type)
			throws DuplicateControllerException;
	
	// Mongo change
	public void mongoCreateSlice(String sliceName, String controllerHostname,
			int controllerPort, String dropPolicy, String passwd,
			String salt, String sliceEmail, String creatorSlice, boolean LLDPSpam, 
			int maxFlowMods, int type)
			throws DuplicateControllerException;
	
	/** Mongo **/
	public void deleteSlice(String SliceName, Boolean preserve) throws InvalidSliceName, ConfigError;
	public void setAdminStatus(String sliceName, boolean status);
	// Mongo change
	public void mongoSetAdminStatus(String sliceName, boolean status);
	
	public boolean isSliceUp(String sliceName);	
	// Mongo change
	public boolean mongoIsSliceUp(String sliceName);
}
