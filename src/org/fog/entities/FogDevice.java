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
import org.fog.utils.TupleFinishDetails;

public class FogDevice extends Datacenter {
	
	private static double RESOURCE_USAGE_COLLECTION_INTERVAL = 10;
	private static double RESOURCE_USAGE_VECTOR_SIZE = 100;
	private static double INPUT_RATE_TIME = 1000;
	
	private Queue<Tuple> outgoingTupleQueue;
	private boolean isOutputLinkBusy;
	private double missRate;
	private double uplinkBandwidth;
	private double latency;
	private Map<String, StreamQuery> streamQueryMap;
	private Map<String, List<String>> queryToOperatorsMap;
	private GeoCoverage geoCoverage;
	private Map<Pair<String, Integer>, Double> inputRateByChildId;
	private Map<Pair<String, Integer>, Integer> inputTuples;
	
	private Queue<Double> outputTupleTimes;
	private Map<String, Queue<Double>> outputTupleTimesByOperator;
	private Map<String, Queue<Double>> intermediateTupleTimesByOperator;
	
	private Map<Pair<String, String>, Double> inputRateByChildOperator;
	private Map<Pair<String, String>, Integer> inputTuplesByChildOperator;
	private Map<Pair<String, String>, Double> tupleLengthByChildOperator;
	private Map<Pair<String, String>, Double> tupleCountsByChildOperator;
	private Map<String, Double> outputTupleLengthsByOperator;
	
	
	private Map<Pair<String, Integer>, Queue<Double>> inputTupleTimes;
	private Map<Pair<String, String>, Queue<Double>> inputTupleTimesByChildOperator;
	private Map<String, Queue<Double>> utilization; 
	/**	
	 * ID of the parent Fog Device
	 */
	private int parentId;
	
	/**
	 * ID of the Controller
	 */
	private int controllerId;
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
			double schedulingInterval,
			double uplinkBandwidth, double latency) throws Exception {
		super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		setGeoCoverage(geoCoverage);
		setCharacteristics(characteristics);
		setVmAllocationPolicy(vmAllocationPolicy);
		setLastProcessTime(0.0);
		setStorageList(storageList);
		setVmList(new ArrayList<Vm>());
		setSchedulingInterval(schedulingInterval);
		setUplinkBandwidth(uplinkBandwidth);
		setLatency(latency);
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
		outgoingTupleQueue = new LinkedList<Tuple>();
		setOutputLinkBusy(false);
		setMissRate(0);
		
		this.inputRateByChildId = new HashMap<Pair<String, Integer>, Double>();
		this.inputTuples = new HashMap<Pair<String, Integer>, Integer>();
		this.inputTupleTimes = new HashMap<Pair<String, Integer>, Queue<Double>>();
		this.inputTupleTimesByChildOperator = new HashMap<Pair<String, String>, Queue<Double>>();
		
		this.utilization = new HashMap<String, Queue<Double>>();
		
		this.tupleLengthByChildOperator = new HashMap<Pair<String, String>, Double>();
		this.tupleCountsByChildOperator = new HashMap<Pair<String, String>, Double>();
		this.inputRateByChildOperator = new HashMap<Pair<String, String>, Double>();
		this.inputTuplesByChildOperator = new HashMap<Pair<String, String>, Integer>();
		
		this.outputTupleTimes = new LinkedList<Double>();
		this.outputTupleTimesByOperator = new HashMap<String, Queue<Double>>();
		this.intermediateTupleTimesByOperator = new HashMap<String, Queue<Double>>();
		this.outputTupleLengthsByOperator = new HashMap<String, Double>();
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
		case FogEvents.UPDATE_TUPLE_QUEUE:
			updateTupleQueue();
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
	
	public boolean checkIfDeviceOverloaded(){
		double load = 0.0;
		for(Pair<String, String> pair : inputRateByChildOperator.keySet()){
			load += getInputTupleRateByChildOperator(pair.getSecond(), pair.getFirst())*tupleLengthByChildOperator.get(pair);
			//OLA1 System.out.println("++++"+load);
		}
		load /= getHost().getTotalMips();
		//OLA1 System.out.println(getName()+"\tLOAD = "+load);
		if(load < 0.95)
			return false;
		else
			return true;
	}
	
	protected void checkCloudletCompletion() {
		boolean cloudletCompleted = false;
		List<? extends Host> list = getVmAllocationPolicy().getHostList();
		for (int i = 0; i < list.size(); i++) {
			Host host = list.get(i);
			for (Vm vm : host.getVmList()) {
				while (vm.getCloudletScheduler().isFinishedCloudlets()) {
					Cloudlet cl = vm.getCloudletScheduler().getNextFinishedCloudlet();
					if (cl != null) {
						
						cloudletCompleted = true;
						
						//System.out.println(CloudSim.clock()+ " : Tuple ID "+((Tuple)cl).getActualTupleId()+" finished on operator "+getOperatorName(cl.getVmId()));
						//OLA System.out.println("Remaining tuples on operator "+getOperatorName(cl.getVmId())+" = "+vm.getCloudletScheduler().runningCloudlets());
						Tuple tuple = (Tuple)cl;
						
						Tuple result = new Tuple(tuple.getQueryId(), FogUtils.generateTupleId(),
								(long) (getStreamQueryMap().get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getExpansionRatio()*tuple.getCloudletLength()),
								tuple.getNumberOfPes(),
								(long) (getStreamQueryMap().get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getFileExpansionRatio()*tuple.getCloudletFileSize()),
								tuple.getCloudletOutputSize(),
								tuple.getUtilizationModelCpu(),
								tuple.getUtilizationModelRam(),
								tuple.getUtilizationModelBw()
								);
						result.setActualTupleId(tuple.getActualTupleId());
						result.setUserId(tuple.getUserId());
						result.setQueryId(tuple.getQueryId());
						result.setEmitTime(tuple.getEmitTime());
						String destoperator = null;
						
						if(getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId())!=null)
							destoperator = getStreamQueryMap().get(tuple.getQueryId()).getNextOperator(tuple.getDestOperatorId()).getName();
						result.setDestOperatorId(destoperator);
						result.setSrcOperatorId(tuple.getDestOperatorId());
						sendToSelf(result);
						
						sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);
					}
				}
			}
		}
		if(cloudletCompleted)
			updateAllocatedMips();
	}
	
	private void updateAllocatedMips(){
		getHost().getVmScheduler().deallocatePesForAllVms();
		for(final Vm vm : getHost().getVmList()){
			if(vm.getCloudletScheduler().runningCloudlets() > 0){
				//getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>(){{add(vm.getMips());}});
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>(){{add((double) getHost().getTotalMips());}});
			}else{
				getHost().getVmScheduler().allocatePesForVm(vm, new ArrayList<Double>(){{add(0.0);}});
			}
		}
		for(final Vm vm : getHost().getVmList()){
			StreamOperator operator = (StreamOperator)vm;
			operator.updateVmProcessing(CloudSim.clock(), getVmAllocationPolicy().getHost(operator).getVmScheduler()
					.getAllocatedMipsForVm(operator));
			// OLA System.out.println("MIPS of "+((StreamOperator)vm).getName()+" = "+getHost().getVmScheduler().getTotalAllocatedMipsForVm(vm));
		}
		
	}
	
	private void updateUtils(){
		if(getName().equals("gateway-0"))
			System.out.println("------->"+getOutputTupleRate("sensor"));
		System.out.println(getName()+" : Traffic Intensity : "+getTrafficIntensity());
		double total = 0;
		for(Vm vm : getHost().getVmList()){
			StreamOperator operator = (StreamOperator)vm;
			if(utilization.get(operator.getName()).size() > RESOURCE_USAGE_VECTOR_SIZE){
				utilization.get(operator.getName()).remove();
			}
			utilization.get(operator.getName()).add(operator.getTotalUtilizationOfCpu(CloudSim.clock()));
			
			//System.out.println(CloudSim.clock()+":\t"+operator.getName()+"\tCLOUDLETS\t"+getUtilizationOfOperator(operator.getName()));
			//System.out.println(CloudSim.clock()+":\t"+getName()+"\tINPUT RATE\t"+getInputRateForDevice());
			
			//System.out.println(CloudSim.clock()+":\t"+operator.getName()+"\t\t"+operator.getCurrentRequestedMips());
			
			total += getUtilizationOfOperator(operator.getName());
			
			/*System.out.println(getName()+"\t"+operator.getName()+"\tINPUT RATE\t"+getInputTupleRate(operator.getName()));
			System.out.println(getName()+"\t"+operator.getName()+"\tINTER RATE\t"+getIntermediateTupleRate(operator.getName()));
			*/
			
			//OLA2 System.out.println(getName()+"\tOUTPUT RATE\t"+getOutputTupleRate());
		}
		//OLA1System.out.println(checkIfDeviceOverloaded());
		//OLA2 displayInputTupleRateByChildOperator();
		

	}
	
	public double getTrafficIntensity(){
		double trafficIntensity = 0;
		for(String operatorName : outputTupleTimesByOperator.keySet()){
			if(outputTupleLengthsByOperator.get(operatorName) != null){
				trafficIntensity += outputTupleLengthsByOperator.get(operatorName)*getOutputTupleRate(operatorName);
			}
			
		}
		trafficIntensity /= getUplinkBandwidth();
		
		return trafficIntensity;
	}
	
	private void updateInputRate(){
		for(Pair<String, Integer> pair : inputTuples.keySet()){
			inputRateByChildId.put(pair, inputTuples.get(pair)/RESOURCE_USAGE_COLLECTION_INTERVAL);
			inputTuples.put(pair, 0);
			//System.out.println(getName()+"\t"+CloudSim.getEntityName(pair.getSecond())+"+"+pair.getFirst()+" --> "+inputRateByChildId.get(pair));
		}
	}
	
	private void updateInputRateByChildOperator(){
		for(Pair<String, String> pair : inputTuplesByChildOperator.keySet()){
			inputRateByChildOperator.put(pair, inputTuplesByChildOperator.get(pair)/RESOURCE_USAGE_COLLECTION_INTERVAL);
			inputTuplesByChildOperator.put(pair, 0);
		}
	}
	
	private void updateResourceUsage(){
		updateUtils();
		updateInputRate();
		//TODO
		updateInputRateByChildOperator();
		
		send(getId(), RESOURCE_USAGE_COLLECTION_INTERVAL, FogEvents.UPDATE_RESOURCE_USAGE);
	}
	
	private double getIntermediateTupleRate(String operatorName){
		Queue<Double> tupleInterTimes = intermediateTupleTimesByOperator.get(operatorName);
		double lastTime = CloudSim.clock() - INPUT_RATE_TIME;
		for(;;){
			if(tupleInterTimes.size() == 0)
				return 0;
			Double time = tupleInterTimes.peek();
			
			if(time < lastTime)
				tupleInterTimes.remove();
			else{
				intermediateTupleTimesByOperator.put(operatorName, tupleInterTimes);
				return (tupleInterTimes.size()/INPUT_RATE_TIME);
			}
		}
	}

	/**
	 * Returns the input rate of tuples from operator operatorName running on Fog device with ID childId
	 * @param childId
	 * @param operatorName
	 */
	private double getInputTupleRate(int childId, String operatorName){
		Queue<Double> tupleInputTimes = inputTupleTimes.get(new Pair<String, Integer>(operatorName, childId));
		double lastTime = CloudSim.clock() - INPUT_RATE_TIME;
		for(;;){
			if(tupleInputTimes.size() == 0)
				return 0;
			Double time = tupleInputTimes.peek();
			
			if(time < lastTime)
				tupleInputTimes.remove();
			else{
				inputTupleTimes.put(new Pair<String, Integer>(operatorName, childId), tupleInputTimes);
				return (tupleInputTimes.size()/INPUT_RATE_TIME);
			}
		}
	}
	
	/**
	 * Returns the input rate of tuples from operator operatorName running on Fog device with ID childId
	 * @param childId
	 * @param operatorName
	 */
	private double getInputTupleRateByChildOperator(String childOpId, String operatorName){
		Queue<Double> tupleInputTimes = inputTupleTimesByChildOperator.get(new Pair<String, String>(operatorName, childOpId));
		double lastTime = CloudSim.clock() - INPUT_RATE_TIME;
		for(;;){
			if(tupleInputTimes.size() == 0)
				return 0;
			Double time = tupleInputTimes.peek();
			
			if(time < lastTime)
				tupleInputTimes.remove();
			else{
				inputTupleTimesByChildOperator.put(new Pair<String, String>(operatorName, childOpId), tupleInputTimes);
				return (tupleInputTimes.size()/INPUT_RATE_TIME);
			}
		}
	}
	
	/**
	 * Returns the input rate for operator operatorName
	 * @param operatorName
	 * @return
	 */
	private double getInputTupleRate(String operatorName){
		double totalInputRate = 0;
		for(Pair<String, Integer> key : inputTupleTimes.keySet()){
			if(operatorName.equals(key.getFirst())){
				totalInputRate += getInputTupleRate(key.getSecond(), operatorName);
			}
		}
		return totalInputRate;
	}
	
	/**
	 * Displays the input rate for operator operatorName by Child operator ID
	 * @param operatorName
	 * @return
	 */
	private void displayInputTupleRateByChildOperator(){
		for(Pair<String, String> key : inputTupleTimesByChildOperator.keySet()){
			System.out.println(getName()+" : INPUT RATE BY CHILD OP ID : "+key.getFirst()+"\t"+key.getSecond()+"\t--->\t"+getInputTupleRateByChildOperator(key.getSecond(), key.getFirst()));
		}
	}
	
	private void updateInputTupleCount(int srcId, String operatorName){
		Pair<String, Integer> pair = new Pair<String, Integer>(operatorName, srcId);
		if(inputTuples.containsKey(pair)){
			inputTuples.put(pair, inputTuples.get(pair)+1);
			inputTupleTimes.get(pair).add(CloudSim.clock());
		}else{
			inputTuples.put(pair, 1);
			inputTupleTimes.put(pair, new LinkedList<Double>());
		}
	}
	
	private void updateInputTupleCountByChildOperator(String operatorName, String childOperatorId){
		Pair<String, String> pair = new Pair<String, String>(operatorName, childOperatorId);
		if(inputTuplesByChildOperator.containsKey(pair)){
			inputTuplesByChildOperator.put(pair, inputTuplesByChildOperator.get(pair)+1);
			inputTupleTimesByChildOperator.get(pair).add(CloudSim.clock());
		}else{
			inputTuplesByChildOperator.put(pair, 1);
			inputTupleTimesByChildOperator.put(pair, new LinkedList<Double>());
		}
	}
	
	private void processQuerySubmit(SimEvent ev) {
		StreamQuery query = (StreamQuery)ev.getData();
		streamQueryMap.put(query.getQueryId(), query);
	}

	private void displayAllocatedMipsForOperators(){
		System.out.println("-----------------------------------------");
		for(Vm vm : getHost().getVmList()){
			StreamOperator operator = (StreamOperator)vm;
			System.out.println("Allocated MIPS for "+operator.getName()+" : "+getHost().getVmScheduler().getTotalAllocatedMipsForVm(operator));
		}
		System.out.println("-----------------------------------------");
	}
	
	private void processTupleArrival(SimEvent ev){
		Tuple tuple = (Tuple)ev.getData();
		//System.out.println(CloudSim.clock()+" : Tuple ID " + tuple.getActualTupleId()+" received by "+getName());
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
		
		if(getHost().getVmList().size() > 0){
			final StreamOperator operator = (StreamOperator)getHost().getVmList().get(0);
			if(CloudSim.clock() > 100){
				getHost().getVmScheduler().deallocatePesForVm(operator);
				//getHost().getVmScheduler().allocatePesForVm(operator, new ArrayList<Double>(){{add(operator.getMips());}});
				getHost().getVmScheduler().allocatePesForVm(operator, new ArrayList<Double>(){{add((double) getHost().getTotalMips());}});
			}
			//displayAllocatedMipsForOperators();
		}
		
		if(Math.random() < missRate)
			return;
		
		if(getName().equals("cloud") && tuple.getDestOperatorId()==null){
			sendNow(getControllerId(), FogEvents.TUPLE_FINISHED, new TupleFinishDetails(tuple.getQueryId(), tuple.getActualTupleId(), tuple.getEmitTime(), CloudSim.clock()));
		}
		
		if(queryToOperatorsMap.containsKey(tuple.getQueryId())){
			if(queryToOperatorsMap.get(tuple.getQueryId()).contains(tuple.getDestOperatorId())){
				int vmId = streamQueryMap.get(tuple.getQueryId()).getOperatorByName(tuple.getDestOperatorId()).getId();
				//System.out.println(CloudSim.clock()+" : Tuple ID " + tuple.getActualTupleId()+" received by "+getName()+" for operator "+tuple.getDestOperatorId());
				tuple.setVmId(vmId);
				updateInputTupleCount(ev.getSource(), tuple.getDestOperatorId());
				updateInputTupleCountByChildOperator(tuple.getDestOperatorId(), tuple.getSrcOperatorId());
				updateTupleLengths(tuple.getSrcOperatorId(), tuple.getDestOperatorId(), tuple.getCloudletLength());
				executeTuple(ev, tuple.getDestOperatorId());
			}else{
				sendUp(tuple);
			}
		}else{
			sendUp(tuple);
		}
	}
	
	private void updateTupleLengths(String srcId, String destOperatorId, long length) {
		Pair<String, String> pair = new Pair<String, String>(destOperatorId, srcId);
		if(tupleLengthByChildOperator.containsKey(pair)){
			double previousTotalLength = tupleLengthByChildOperator.get(pair)*tupleCountsByChildOperator.get(pair);
			tupleLengthByChildOperator.put(pair, (previousTotalLength+length)/(tupleCountsByChildOperator.get(pair)+1));
			tupleCountsByChildOperator.put(pair, tupleCountsByChildOperator.get(pair)+1);
		}else{
			tupleLengthByChildOperator.put(pair, (double)length);
			tupleCountsByChildOperator.put(pair, 1.0);
		}
		
	}

	private void processSensorJoining(SimEvent ev){
		//TODO
		send(ev.getSource(), CloudSim.getMinTimeBetweenEvents(), FogEvents.TUPLE_ACK);
	}
	
	private void executeTuple(SimEvent ev, String operatorId){
		Tuple tuple = (Tuple)ev.getData();
		processCloudletSubmit(ev, false);
		updateAllocatedMips();
	}
	
	private void processOperatorArrival(SimEvent ev){
		
		StreamOperator operator = (StreamOperator)ev.getData();
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
		intermediateTupleTimesByOperator.put(operator.getName(), new LinkedList<Double>());
		outputTupleTimesByOperator.put(operator.getName(), new LinkedList<Double>());
	}
	
	private void processOperatorRelease(SimEvent ev){
		this.processVmMigrate(ev, false);
	}
	
	public boolean isAncestorOf(FogDevice dev){
		if(this.geoCoverage.covers(dev.getGeoCoverage()))
			return true;
		return false;
	}
	
	private void updateTupleQueue(){
		if(!getOutgoingTupleQueue().isEmpty()){
			Tuple tuple = getOutgoingTupleQueue().poll();
			sendUpFreeLink(tuple);
		}else{
			setOutputLinkBusy(false);
		}
	}
	
	private void sendUpFreeLink(Tuple tuple){
		//System.out.println(CloudSim.clock()+"\tSending tuple ID "+tuple.getActualTupleId()+" from "+getName());
		//System.out.println(CloudSim.clock()+" : Tuple ID " + tuple.getActualTupleId()+" being sent up FREE LINK.");
		double networkDelay = tuple.getCloudletFileSize()/getUplinkBandwidth();
		setOutputLinkBusy(true);
		send(getId(), networkDelay, FogEvents.UPDATE_TUPLE_QUEUE);
		send(parentId, networkDelay+latency, FogEvents.TUPLE_ARRIVAL, tuple);
	}
	
	private double getOutputTupleRate(String operatorName){
		
		if(!outputTupleTimesByOperator.containsKey(operatorName))
			return 0;
		//System.out.println(CloudSim.clock()+"\t"+operatorName+"\t"+outputTupleTimesByOperator.get(operatorName));
		Queue<Double> tupleOutputTimes = outputTupleTimesByOperator.get(operatorName);
		double lastTime = CloudSim.clock() - INPUT_RATE_TIME;
		for(;;){
			if(tupleOutputTimes.size() == 0)
				return 0;
			Double time = tupleOutputTimes.peek();
			
			if(time < lastTime)
				tupleOutputTimes.remove();
			else{
				outputTupleTimesByOperator.put(operatorName, tupleOutputTimes);
				return (tupleOutputTimes.size()/INPUT_RATE_TIME);
			}
		}
	}
	
	private void sendUp(Tuple tuple){
		//System.out.println(CloudSim.clock()+" : Tuple ID " + tuple.getActualTupleId()+" being sent up.");
		outputTupleTimes.add(CloudSim.clock());
		if(outputTupleTimesByOperator.containsKey(tuple.getSrcOperatorId()))
			outputTupleTimesByOperator.get(tuple.getSrcOperatorId()).add(CloudSim.clock());
		else{
			outputTupleTimesByOperator.put(tuple.getSrcOperatorId(), new LinkedList<Double>());
			outputTupleTimesByOperator.get(tuple.getSrcOperatorId()).add(CloudSim.clock());
		}
		
		outputTupleLengthsByOperator.put(tuple.getSrcOperatorId(), (double) tuple.getCloudletFileSize());
		/*if(intermediateTupleTimesByOperator.get(tuple.getSrcOperatorId())==null){
			intermediateTupleTimesByOperator.put(tuple.getSrcOperatorId(), new LinkedList<Double>());
		}
		intermediateTupleTimesByOperator.get(tuple.getSrcOperatorId()).add(CloudSim.clock());
		*/
		if(parentId > 0){
			if(!isOutputLinkBusy()){
				sendUpFreeLink(tuple);
			}else{
				outgoingTupleQueue.add(tuple);
			}
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

	public double getUplinkBandwidth() {
		return uplinkBandwidth;
	}

	public void setUplinkBandwidth(double uplinkBandwidth) {
		this.uplinkBandwidth = uplinkBandwidth;
	}

	public double getLatency() {
		return latency;
	}

	public void setLatency(double latency) {
		this.latency = latency;
	}

	public Queue<Tuple> getOutgoingTupleQueue() {
		return outgoingTupleQueue;
	}

	public void setOutgoingTupleQueue(Queue<Tuple> outgoingTupleQueue) {
		this.outgoingTupleQueue = outgoingTupleQueue;
	}

	public boolean isOutputLinkBusy() {
		return isOutputLinkBusy;
	}

	public void setOutputLinkBusy(boolean isOutputLinkBusy) {
		this.isOutputLinkBusy = isOutputLinkBusy;
	}

	public int getControllerId() {
		return controllerId;
	}

	public void setControllerId(int controllerId) {
		this.controllerId = controllerId;
	}

	
}
