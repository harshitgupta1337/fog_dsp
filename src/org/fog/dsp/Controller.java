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
import org.fog.utils.TupleEmitTimes;
import org.fog.utils.TupleFinishDetails;

public class Controller extends SimEntity{

	private OperatorPlacement operatorPlacement;
	
	private List<FogDevice> fogDevices;
	
	private List<StreamQuery> queries;
	
	public Controller(String name, List<FogDevice> fogDevices) {
		super(name);
		this.queries = new ArrayList<StreamQuery>();
		for(FogDevice fogDevice : fogDevices){
			fogDevice.setControllerId(getId());
		}
		setFogDevices(fogDevices);
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
		case FogEvents.TUPLE_FINISHED:
			processTupleFinished(ev);
			break;
		}
		
	}

	private void processTupleFinished(SimEvent ev) {
		TupleFinishDetails details = (TupleFinishDetails)ev.getData();
		TupleEmitTimes.setEmitTime(details.getQueryId(), details.getActualTupleId(), details.getFinishTime()-details.getEmitTime());
		System.out.println(details.getActualTupleId()+"\t---->\t"+(details.getFinishTime()-details.getEmitTime()));

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
