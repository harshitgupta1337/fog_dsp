package org.fog.dsp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.entities.FogDevice;
import org.fog.entities.StreamOperator;
import org.fog.utils.FogEvents;

public class Controller extends SimEntity{

	private OperatorPlacement operatorPlacement;
	
	private List<FogDevice> fogDevices;
	
	private List<StreamQuery> queries;
	
	public Controller(String name, List<FogDevice> fogDevices) {
		super(name);
		this.queries = new ArrayList<StreamQuery>();
		setFogDevices(fogDevices);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void startEntity() {
		// TODO Auto-generated method stub
		for(StreamQuery query : queries){
			processQuerySubmit(query);
		}
	}

	@Override
	public void processEvent(SimEvent ev) {
		switch(ev.getTag()){
		case FogEvents.QUERY_SUBMIT:
			processQuerySubmit(ev);
			break;
		}
		
	}

	@Override
	public void shutdownEntity() {
		// TODO Auto-generated method stub
		
	}

	public void submitStreamQuery(StreamQuery streamQuery){
		//processQuerySubmit(streamQuery);
		queries.add(streamQuery);
	}
	
	private void processQuerySubmit(SimEvent ev){
		StreamQuery query = (StreamQuery) ev.getData();
		Map<String, Integer> allocationMap = (new OperatorPlacementSimple(fogDevices, query)).getOperatorToDeviceMap();
		for(String operatorName : allocationMap.keySet()){
			StreamOperator operator = query.getOperatorByName(operatorName);
			sendNow(allocationMap.get(operatorName), FogEvents.LAUNCH_OPERATOR, operator);
		}
	}
	
	private void processQuerySubmit(StreamQuery query){
		Map<String, Integer> allocationMap = (new OperatorPlacementSimple(fogDevices, query)).getOperatorToDeviceMap();
		for(String operatorName : allocationMap.keySet()){
			StreamOperator operator = query.getOperatorByName(operatorName);
			System.out.println("Operator "+operator.getName()+" has been placed on "+allocationMap.get(operatorName));
			System.out.println(CloudSim.getEntityName(allocationMap.get(operatorName)));
			System.out.println(operator);
			//
			sendNow(allocationMap.get(operatorName), FogEvents.QUERY_SUBMIT, query);
			
			sendNow(allocationMap.get(operatorName), FogEvents.LAUNCH_OPERATOR, operator);
		}
	}
	
	public OperatorPlacement getOperatorPlacement() {
		return operatorPlacement;
	}

	public void setOperatorPlacement(OperatorPlacement operatorPlacement) {
		this.operatorPlacement = operatorPlacement;
	}

	public List<FogDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}
}
