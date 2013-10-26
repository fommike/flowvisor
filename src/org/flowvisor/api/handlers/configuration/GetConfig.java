package org.flowvisor.api.handlers.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.flowvisor.api.handlers.ApiHandler;
import org.flowvisor.api.handlers.HandlerUtils;
import org.flowvisor.config.ConfigError;
import org.flowvisor.config.FlowSpace;
import org.flowvisor.config.FlowvisorImpl;
import org.flowvisor.config.SliceImpl;
import org.flowvisor.config.SwitchImpl;
import org.flowvisor.exceptions.MissingRequiredField;
import org.flowvisor.flows.FlowSpaceUtil;
import org.flowvisor.openflow.protocol.FVMatch;

import com.thetransactioncompany.jsonrpc2.JSONRPC2Error;
import com.thetransactioncompany.jsonrpc2.JSONRPC2ParamsType;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response;

public class GetConfig implements ApiHandler<Map<String, Object>> {

	@Override
	public JSONRPC2Response process(Map<String, Object> params) {
		JSONRPC2Response resp = null;
		HashMap<String, Object> configs = new HashMap<String, Object>();
		
		
		try {
			String sliceName = HandlerUtils.<String>fetchField(SLICENAME, params, false, null);
			String dpidStr = HandlerUtils.<String>fetchField(FlowSpace.DPID, params, false, null);
	
			/*
			configs.put(TRACK, FlowvisorImpl.getProxy().gettrack_flows());
			configs.put(STATSDESC, FlowvisorImpl.getProxy().getstats_desc_hack());
			configs.put(TOPOCTRL, FlowvisorImpl.getProxy().getTopologyServer());
			configs.put(FSCACHE, FlowvisorImpl.getProxy().getFlowStatsCache());
			*/
			
			///*
			configs.put(TRACK, FlowvisorImpl.getProxy().mongoGetTrackFlows());
			configs.put(STATSDESC, FlowvisorImpl.getProxy().mongoGetStatsDescHack());
			configs.put(TOPOCTRL, FlowvisorImpl.getProxy().mongoGetTopologyServer());
			configs.put(FSCACHE, FlowvisorImpl.getProxy().mongoGetFlowStatsCache());
			//*/
			
			addFloodPerms(dpidStr, configs);
			addFlowmodLimits(sliceName, dpidStr, configs);
			
			resp = new JSONRPC2Response(configs, 0);
		} catch (ConfigError e) {
			resp = new JSONRPC2Response(new JSONRPC2Error(JSONRPC2Error.INTERNAL_ERROR.getCode(), 
					cmdName() + ": Unable to fetch/set config : " + e.getMessage()), 0);
		} catch (ClassCastException e) {
			resp = new JSONRPC2Response(new JSONRPC2Error(JSONRPC2Error.INVALID_PARAMS.getCode(), 
					cmdName() + ": " + e.getMessage()), 0);
		} catch (MissingRequiredField e) {
			resp = new JSONRPC2Response(new JSONRPC2Error(JSONRPC2Error.INVALID_PARAMS.getCode(), 
					cmdName() + ": " + e.getMessage()), 0);
		} 
		return resp;
	}

    @SuppressWarnings("unchecked")
	private void addFlowmodLimits(String sliceName, String dpidStr,
			HashMap<String, Object> configs) throws MissingRequiredField, ConfigError {
    	
    	HashMap<String, HashMap<String, Object>> list = new HashMap<String,HashMap<String,Object>>();
    	HashMap<String, Object> subconfs = new HashMap<String, Object>();
		if (sliceName != null && dpidStr != null) {
			long dpid = FlowSpaceUtil.parseDPID(dpidStr);
			if (dpid == FVMatch.ANY_DPID) {
				//subconfs.put("any", SliceImpl.getProxy().getMaxFlowMods(sliceName));
				subconfs.put("any", SliceImpl.getProxy().mongoGetMaxFlowMods(sliceName));
			}
			else {
				//subconfs.put(dpidStr, SwitchImpl.getProxy().getMaxFlowMods(sliceName, dpid));
				subconfs.put(dpidStr, SwitchImpl.getProxy().mongoGetMaxFlowMods(sliceName, dpid));
			}
			list.put(sliceName,subconfs);
		} else if (sliceName != null && dpidStr == null) {
			//subconfs.put("any", SliceImpl.getProxy().getMaxFlowMods(sliceName));
			subconfs.put("any", SliceImpl.getProxy().mongoGetMaxFlowMods(sliceName));
			for (String dpid : HandlerUtils.getAllDevices()) {
				//subconfs.put(dpid, SwitchImpl.getProxy().getMaxFlowMods(sliceName, FlowSpaceUtil.parseDPID(dpid)));
				subconfs.put(dpid, SwitchImpl.getProxy().mongoGetMaxFlowMods(sliceName, FlowSpaceUtil.parseDPID(dpid)));
			}
			list.put(sliceName, subconfs);
		} else if (dpidStr != null && sliceName == null) {
			long dpid = FlowSpaceUtil.parseDPID(dpidStr);
			//List<String> slices = SliceImpl.getProxy().getAllSliceNames();
			List<String> slices = SliceImpl.getProxy().mongoGetAllSliceNames();
			for (String slice : slices) {
				//subconfs.put(dpidStr, SwitchImpl.getProxy().getMaxFlowMods(slice, dpid));
				subconfs.put(dpidStr, SwitchImpl.getProxy().mongoGetMaxFlowMods(slice, dpid));
				list.put(slice, (HashMap<String, Object>) subconfs.clone());
				subconfs.clear();
			}
		} else {
			//List<String> slices = SliceImpl.getProxy().getAllSliceNames();
			List<String> slices = SliceImpl.getProxy().mongoGetAllSliceNames();
			for (String slice : slices) {
				subconfs.clear();
				//subconfs.put("any", SliceImpl.getProxy().getMaxFlowMods(slice));
				subconfs.put("any", SliceImpl.getProxy().mongoGetMaxFlowMods(slice));
				/** Temporary stuff **/
				if (slice.equals("fvadmin")) {
					for (String dpid : HandlerUtils.getAllDevices()) {
						subconfs.put(dpid, -1);
					}
					list.put(slice,(HashMap<String, Object>)subconfs.clone());
				} else {
					for (String dpid : HandlerUtils.getAllDevices()) {
						//subconfs.put(dpid, SwitchImpl.getProxy().getMaxFlowMods(slice, FlowSpaceUtil.parseDPID(dpid)));
						subconfs.put(dpid, SwitchImpl.getProxy().mongoGetMaxFlowMods(slice, FlowSpaceUtil.parseDPID(dpid)));
					}
					list.put(slice,(HashMap<String, Object>)subconfs.clone());
				}
			}
		}
		configs.put(MAX, list);	
	}

	private void addFloodPerms(String dpidStr,
			HashMap<String, Object> configs) throws ConfigError {
		HashMap<String, Object> subconfs = new HashMap<String, Object>();
		if (dpidStr != null) {
			//subconfs.put(SLICENAME, SwitchImpl.getProxy().getFloodPerm(FlowSpaceUtil.parseDPID(dpidStr)));
			subconfs.put(SLICENAME, SwitchImpl.getProxy().mongoGetFloodPerm(FlowSpaceUtil.parseDPID(dpidStr)));
			
			subconfs.put(FlowSpace.DPID, dpidStr);
		} else {
			subconfs.put(FlowSpace.DPID, "all");
			//subconfs.put(SLICENAME, FlowvisorImpl.getProxy().getFloodPerm());
			subconfs.put(SLICENAME, FlowvisorImpl.getProxy().mongoGetFloodPerm());
		}
		configs.put(FLOOD, subconfs);
	}

	@Override
	public JSONRPC2ParamsType getType() {
		return JSONRPC2ParamsType.OBJECT;
	}

	@Override
	public String cmdName() {
		return "get-config";
	}
}
