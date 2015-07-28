/*
 * Title: CloudSim Toolkit Description: CloudSim (Cloud Simulation) Toolkit for Modeling and
 * Simulation of Clouds Licence: GPL - http://www.gnu.org/copyleft/gpl.html
 * 
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.fog.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.sdn.power.PowerUtilizationHistoryEntry;
import org.fog.dsp.StreamQuery;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.GeoCoverage;

public class FogDevice extends Datacenter {
	
	private static double INPUT_RATE_CALC_INTERVAL = 200;
	
	private double missRate;
	private Map<String, StreamQuery> streamQueryMap;
	private Map<String, List<String>> queryToOperatorsMap;
	private GeoCoverage geoCoverage;
	private Map<Pair<String, Integer>, Double> inputRateByChildId;
	private Map<Pair<String, Integer>, Integer> inputTuples;
	private Queue<Double> utilization; 
	/**	
	 * ID of the parent Fog Device
	 */
	private int parentId;
	
	/**
	 * IDs of the children Fog devices
	 */
	private List<Integer> childrenIds;

		
	public FogDevice(
			String name, 
			GeoCoverage geoCoverage,
			FogDeviceCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy,
			List<Storage> storageList,
			double schedulingInterval) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		setGeoCoverage(geoCoverage);
		setCharacteristics(characteristics);
		setVmAllocationPolicy(vmAllocationPolicy);
		setLastProcessTime(0.0);
		setStorageList(storageList);
		setVmList(new ArrayList<Vm>());
		setSchedulingInterval(schedulingInterval);

		for (Host host : getCharacteristics().getHostList()) {
			host.setDatacenter(this);
		}

		// If this resource doesn't have any PEs then no useful at all
		if (getCharacteristics().getNumberOfPes() == 0) {
			throw new Exception(super.getName()
					+ " : Error - this entity has no PEs. Therefore, can't process any Cloudlets.");
		}

		// stores id of this class
		getCharacteristics().setId(super.getId());
		
		streamQueryMap = new HashMap<String, StreamQuery>();
		queryToOperatorsMap = new HashMap<String, List<String>>();
		
		setMissRate(0);
		
		this.inputRateByChildId = new HashMap<Pair<String, Integer>, Double>();
		this.inputTuples = new HashMap<Pair<String, Integer>, Integer>();
		
		this.utilization = new LinkedList<Double>();
		
	}

	/**
	 * Overrides this method when making a new and different type of resource. <br>
	 * <b>NOTE:</b> You do not need to override {@link #body()} method, if you use this method.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void registerOtherEntity() {
		updateInputRate();
		updateUtils();
	}
	
	@Override
	protected void processOtherEvent(SimEvent ev) {
		switch(ev.getTag()){
		
		case FogEvents.TUPLE_ARRIVAL:
			/*for(Vm vm : getHost().getVmList()){
				System.out.println(getName()+" ----> "+((StreamOperator)vm).getName()+"\t : "+vm.getTotalUtilizationOfCpu(CloudSim.clock()));
			}*/
			processTupleArrival(ev);
			break;
		case FogEvents.LAUNCH_OPERATOR:
			processOperatorArrival(ev);
			break;
		case FogEvents.RELEASE_OPERATOR:
			processOperatorRelease(ev);
			break;
		case FogEvents.SENSOR_JOINED:
			processSensorJoining(ev);
			break;
		case FogEvents.QUERY_SUBMIT:
			processQuerySubmit(ev);
			break;
		case FogEvents.CALCULATE_INPUT_RATE:
			updateInputRate();
			break;
		case FogEvents.CALCULATE_UTIL:
			updateUtils();
		default:
			break;
		}
	}
	
	private void updateUtils(){
		if(utilization.size()>10){
			utilization.remove();
		}
		double totalRequestedMips = 0;
		for(Vm operator : getHost().getVmList()){
			for(Double mips : (((StreamOperator)operator).getCurrentRequestedMips())){
				totalRequestedMips += mips;
			}
		}
		utilization.add(totalRequestedMips/getHost().getTotalMips());
		send(getId(), 10, FogEvents.CALCULATE_UTIL);
		if(getName().equals("level1-13"))
			System.out.println(getName()+"\t"+utilization);
	}
	
	private void updateInputRate(){
		for(Pair<String, Integer> pair : inputTuples.keySet()){
			inputRateByChildId.put(pair, inputTuples.get(pair)/INPUT_RATE_CALC_INTERVAL);
			inputTuples.put(pair, 0);
			System.out.println(getName()+"\t"+CloudSim.getEntityName(pair.getSecond())+"+"+pair.getFirst()+" --> "+inputRateByChildId.get(pair));
		}
		send(getId(), INPUT_RATE_CALC_INTERVAL, FogEvents.CALCULATE_INPUT_RATE);
	}
	
	private void updateInputTupleCount(int srcId, String operatorName){
		Pair<String, Integer> pair = new Pair<String, Integer>(operatorName, srcId);
		if(inputTuples.containsKey(pair)){
			inputTuples.put(pair, inputTuples.get(pair)+1);
		}else{
			inputTuples.put(pair, 1);
		}
	}
	
	private void processQuerySubmit(SimEvent ev) {
		StreamQuery query = (StreamQuery)ev.getData();
		streamQueryMap.put(query.getQueryId(), query);
	}

	private void processTupleArrival(SimEvent ev){
		Tuple tuple = (Tuple)ev.getData();
		//System.out.println(getName()+": \tTuple arrived at time \t"+CloudSim.clock()+"\tTuple Id : "+tuple.getCloudletId()+"\tDest Op : "+tuple.getDestOperatorId());
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
		
		/*List<PowerUtilizationHistoryEntry> list = (((StreamOperatorScheduler)this.getHost().getVmScheduler()).getUtilizationHisotry());
		if(list != null){
			for(PowerUtilizationHistoryEntry entry : list){
				if(CloudSim.clock() - entry.startTime < 100 && this.getName().equals("level1-13"))
					System.out.println(">>>>>"+CloudSim.clock()+"\t"+entry.startTime+"\t"+entry.usedMips);
			}
		}*/
		/*
		StreamOperator op = (StreamOperator) getHost().getVmList().get(0);
		System.out.println(op.getCloudletScheduler().getCurrentRequestedMips());
		*/

		
		if(Math.random() < missRate)
			return;
		/*
		for(Vm vm : getHost().getVmList()){
			System.out.println(getName()+"\t"+((StreamOperator)vm).getName()+"\t"+vm.getCurrentRequestedMips());
		}
		*/
		//System.out.println("XXX"+getHost().getVmList().get(0).getCurrentRequestedTotalMips());
		if(queryToOperatorsMap.containsKey(tuple.getQueryId())){
			if(queryToOperatorsMap.get(tuple.getQueryId()).contains(tuple.getDestOperatorId())){
				int vmId = streamQueryMap.get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getId();
				tuple.setVmId(vmId);
				updateInputTupleCount(ev.getSource(), tuple.getDestOperatorId());
				Tuple result = executeTuple(ev, tuple.getDestOperatorId());
				if(result != null)
					sendToSelf(result);
			}else{
				sendUp(tuple);
			}
		}else{
			sendUp(tuple);
		}
	}
	
	
	private void processSensorJoining(SimEvent ev){
		//TODO
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
	}
	
	private Tuple executeTuple(SimEvent ev, String operatorId){
		processCloudletSubmit(ev, false);
		
		Tuple tuple = (Tuple) ev.getData();
		// some code to make the device busy executing the tuple
		
		Tuple result = new Tuple(tuple.getQueryId(), FogUtils.generateTupleId(),
				(long) (getStreamQueryMap().get(tuple.getQueryId()).getOperatorByName(operatorId).getExpansionRatio()*tuple.getCloudletLength()),
				tuple.getNumberOfPes(),
				tuple.getCloudletFileSize(),
				tuple.getCloudletOutputSize(),
				tuple.getUtilizationModelCpu(),
				tuple.getUtilizationModelRam(),
				tuple.getUtilizationModelBw()
				);
		result.setUserId(tuple.getUserId());
		result.setQueryId(tuple.getQueryId());
		String destoperator = null;
		if(getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId())!=null)
			destoperator = getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId()).getName();
		result.setDestOperatorId(destoperator);
		//System.out.println(tuple.getDestOperatorId() + "\t" + destoperator);
		
		//System.out.println(result.getDestOperatorId());
		//System.out.println(tuple.getCloudletId()+"\t"+result.getCloudletId()+"\t"+CloudSim.getEntityName(ev.getSource())+"\t"+tuple.getDestOperatorId()+"\t"+result.getDestOperatorId());
		
		return result;
	}
	
	private void processOperatorArrival(SimEvent ev){
		
		StreamOperator operator = (StreamOperator)ev.getData();
		//System.out.println("OPERATOR "+operator.getName()+" ARRIVED");
		String queryId = operator.getQueryId();
		if(!queryToOperatorsMap.containsKey(queryId)){
			queryToOperatorsMap.put(queryId, new ArrayList<String>());
		}
		queryToOperatorsMap.get(queryId).add(operator.getName());
		
		operator.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(operator).getVmScheduler()
				.getAllocatedMipsForVm(operator));
	}
	
	private void processOperatorRelease(SimEvent ev){
		this.processVmMigrate(ev, false);
	}
	
	public boolean isAncestorOf(FogDevice dev){
		if(this.geoCoverage.covers(dev.getGeoCoverage()))
			return true;
		return false;
	}
	
	private void sendUp(Tuple tuple){
		if(parentId > 0)
			send(parentId, CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ARRIVAL, tuple);
	}
	
	private void sendToSelf(Tuple tuple){
		send(getId(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ARRIVAL, tuple);
	}
	
	public Host getHost(){
		return getHostList().get(0);
	}
	
	public int getParentId() {
		return parentId;
	}

	public void setParentId(int parentId) {
		this.parentId = parentId;
	}

	public List<Integer> getChildrenIds() {
		return childrenIds;
	}

	public void setChildrenIds(List<Integer> childrenIds) {
		this.childrenIds = childrenIds;
	}

	public Map<String, StreamQuery> getStreamQueryMap() {
		return streamQueryMap;
	}

	public void setStreamQueryMap(Map<String, StreamQuery> streamQueryMap) {
		this.streamQueryMap = streamQueryMap;
	}

	public GeoCoverage getGeoCoverage() {
		return geoCoverage;
	}

	public void setGeoCoverage(GeoCoverage geoCoverage) {
		this.geoCoverage = geoCoverage;
	}

	public double getMissRate() {
		return missRate;
	}

	public void setMissRate(double missRate) {
		this.missRate = missRate;
	}

	
}
