package org.fog.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.math3.util.Pair;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.dsp.StreamQuery;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.GeoCoverage;

public class FogDevice extends Datacenter {
	
	private static double RESOURCE_USAGE_COLLECTION_INTERVAL = 20;
	private static double RESOURCE_USAGE_VECTOR_SIZE = 10;
	
	private double missRate;
	private Map<String, StreamQuery> streamQueryMap;
	private Map<String, List<String>> queryToOperatorsMap;
	private GeoCoverage geoCoverage;
	private Map<Pair<String, Integer>, Double> inputRateByChildId;
	private Map<Pair<String, Integer>, Integer> inputTuples;
	private Map<String, Queue<Double>> utilization; 
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
		
		this.utilization = new HashMap<String, Queue<Double>>();
		
	}

	/**
	 * Overrides this method when making a new and different type of resource. <br>
	 * <b>NOTE:</b> You do not need to override {@link #body()} method, if you use this method.
	 * 
	 * @pre $none
	 * @post $none
	 */
	protected void registerOtherEntity() {
		updateResourceUsage();
	}
	
	@Override
	protected void processOtherEvent(SimEvent ev) {
		switch(ev.getTag()){
		case FogEvents.TUPLE_ARRIVAL:
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
		case FogEvents.UPDATE_RESOURCE_USAGE:
			updateResourceUsage();
			break;
		default:
			break;
		}
	}
	
	/**
	 * Calculates utilization of each operator.
	 * @param operatorName
	 * @return
	 */
	public double getUtilizationOfOperator(String operatorName){
		double total = 0;
		for(Double d : utilization.get(operatorName)){
			total += d;
		}
		return total/utilization.get(operatorName).size();
	}
	
	public String getOperatorName(int vmId){
		for(Vm vm : this.getHost().getVmList()){
			if(vm.getId() == vmId)
				return ((StreamOperator)vm).getName();
		}
		return null;
	}
	
	protected void checkCloudletCompletion() {
		List<? extends Host> list = getVmAllocationPolicy().getHostList();
		for (int i = 0; i < list.size(); i++) {
			Host host = list.get(i);
			for (Vm vm : host.getVmList()) {
				while (vm.getCloudletScheduler().isFinishedCloudlets()) {
					//System.out.println("Inside checkCloudletCompletion for VM "+((StreamOperator)vm).getName());
					Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
					if (cl != null) {
						//System.out.println("Actual Tuple ID "+((Tuple)cl).getActualTupleId()+" finished on operator "+getOperatorName(cl.getVmId()) + " at time "+CloudSim.clock());
						Tuple tuple = (Tuple)cl;

						Tuple result = new Tuple(tuple.getQueryId(), FogUtils.generateTupleId(),
								(long) (getStreamQueryMap().get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getExpansionRatio()*tuple.getCloudletLength()),
								tuple.getNumberOfPes(),
								tuple.getCloudletFileSize(),
								tuple.getCloudletOutputSize(),
								tuple.getUtilizationModelCpu(),
								tuple.getUtilizationModelRam(),
								tuple.getUtilizationModelBw()
								);
						result.setActualTupleId(tuple.getActualTupleId());
						result.setUserId(tuple.getUserId());
						result.setQueryId(tuple.getQueryId());
						String destoperator = null;
						
						if(getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId())!=null)
							destoperator = getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId()).getName();
						result.setDestOperatorId(destoperator);
						
						sendToSelf(result);
						
						sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
					}
				}
			}
		}
	}
	
	private void updateUtils(){
		double total = 0;
		for(Vm vm : getHost().getVmList()){
			StreamOperator operator = (StreamOperator)vm;
			if(utilization.get(operator.getName()).size() > RESOURCE_USAGE_VECTOR_SIZE){
				utilization.get(operator.getName()).remove();
			}
			utilization.get(operator.getName()).add(operator.getTotalUtilizationOfCpu(CloudSim.clock()));
			
			System.out.println(operator.getName()+"--->\t"+getUtilizationOfOperator(operator.getName()));
			//System.out.println(getMissRate()+"\t"+operator.getName()+"--->\t"+utilization.get(operator.getName()));
			
			total += getUtilizationOfOperator(operator.getName());
		}
		
		
	}
	
	private void updateInputRate(){
		for(Pair<String, Integer> pair : inputTuples.keySet()){
			inputRateByChildId.put(pair, inputTuples.get(pair)/RESOURCE_USAGE_COLLECTION_INTERVAL);
			inputTuples.put(pair, 0);
			//System.out.println(getName()+"\t"+CloudSim.getEntityName(pair.getSecond())+"+"+pair.getFirst()+" --> "+inputRateByChildId.get(pair));
		}
	}
	
	private void updateResourceUsage(){
		updateUtils();
		updateInputRate();
		send(getId(), RESOURCE_USAGE_COLLECTION_INTERVAL, FogEvents.UPDATE_RESOURCE_USAGE);
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
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
		
		//System.out.println(getName() + " received actual tuple ID " + tuple.getActualTupleId() + " for " + tuple.getDestOperatorId() + " at time "+CloudSim.clock());
		/*for(Vm vm : getHost().getVmList()){
			//System.out.println(getName()+"\t"+((StreamOperator)vm).getName()+"\t"+vm.getCurrentAllocatedMips()+"\t"+vm.getCurrentRequestedMips());
			//System.out.println(getName()+"\t"+((StreamOperator)vm).getName()+"\t"+vm.getCloudletScheduler().getCurrentMipsShare());
		}*/
		
		if(Math.random() < missRate)
			return;
		
		if(getName().equals("cloud") && tuple.getDestOperatorId()==null){
			//System.out.println(CloudSim.clock()+" : Tuple ID "+tuple.getActualTupleId()+" arrived at cloud");
			//System.out.println(tuple.getActualTupleId()+"\t---->\t"+(CloudSim.clock()-TupleEmitTimes.getEmitTime(tuple.getActualTupleId())));
		}
			
		
		//System.out.println("Tuple ID "+tuple.getActualTupleId()+" arrived at "+getName()+"for operator "+tuple.getDestOperatorId()+" at time "+CloudSim.clock());
		
		if(queryToOperatorsMap.containsKey(tuple.getQueryId())){
			if(queryToOperatorsMap.get(tuple.getQueryId()).contains(tuple.getDestOperatorId())){
				int vmId = streamQueryMap.get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getId();
				
				StreamOperator operator = streamQueryMap.get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId());
				//System.out.println("Allocated mips for operator "+operator.getName()+" = "+getVmAllocationPolicy().getHost(operator).getVmScheduler()
				//.getAllocatedMipsForVm(operator));
				tuple.setVmId(vmId);
				updateInputTupleCount(ev.getSource(), tuple.getDestOperatorId());
				executeTuple(ev, tuple.getDestOperatorId());
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
	
	private void executeTuple(SimEvent ev, String operatorId){
		processCloudletSubmit(ev, false);
		
		/*Tuple tuple = (Tuple) ev.getData();
		
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
		result.setDestOperatorId(destoperator);*/
	}
	
	private void processOperatorArrival(SimEvent ev){
		
		StreamOperator operator = (StreamOperator)ev.getData();
		//System.out.println("OPERATOR "+operator.getName()+" ARRIVED");
		String queryId = operator.getQueryId();
		if(!queryToOperatorsMap.containsKey(queryId)){
			queryToOperatorsMap.put(queryId, new ArrayList<String>());
		}
		queryToOperatorsMap.get(queryId).add(operator.getName());
		getVmList().add(operator);
		if (operator.isBeingInstantiated()) {
			operator.setBeingInstantiated(false);
		}
		utilization.put(operator.getName(), new LinkedList<Double>());
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
		if(parentId > 0){
			//System.out.println(getName() + " is sending UP actual tuple Id "+tuple.getActualTupleId());
			send(parentId, CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ARRIVAL, tuple);
		}
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
