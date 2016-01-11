package org.fog.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.dsp.StreamQuery;
import org.fog.utils.CanBeSentResult;
import org.fog.utils.FogUtils;
import org.fog.utils.GeoCoverage;
import org.fog.utils.OperatorSetComparator;
import org.fog.utils.ResourceUsageDetails;

public class FogDeviceCollector extends FogDevice{

	Map<Integer, ResourceUsageDetails> childResourceUsages;
	
	public FogDeviceCollector(String name, GeoCoverage geoCoverage,
			FogDeviceCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList,
			double schedulingInterval, double uplinkBandwidth, double latency)
			throws Exception {
		super(name, geoCoverage, characteristics, vmAllocationPolicy, storageList,
				schedulingInterval, uplinkBandwidth, latency);
		childResourceUsages = new HashMap<Integer, ResourceUsageDetails>();
	}
	
	protected boolean allChildrenDone(){
		for(int childId : getChildrenIds()){
			if(!childResourceUsages.containsKey(childId))
				return false;
		}
		return true;
	}
	
	@Override
	protected void processResourceUsage(SimEvent ev) {
		ResourceUsageDetails resourceUsageDetails = (ResourceUsageDetails)ev.getData();
		int childId1 = ev.getSource();
		childResourceUsages.put(childId1, resourceUsageDetails);
		
		if(allChildrenDone()){
			double currentCpuLoad = calculateCpuLoad()*getHost().getTotalMips();
			double currentNwLoad = getTrafficIntensity()*getUplinkBandwidth();
			
			for(Integer childId : childResourceUsages.keySet()){
				resourceUsageDetails = childResourceUsages.get(childId);
				
				Map<String, List<StreamOperator>> map = new HashMap<String, List<StreamOperator>>();
				for(Vm vm : getHost().getVmList()){
					StreamOperator streamOperator = (StreamOperator)vm;
					if(map.containsKey(streamOperator.getQueryId()))
						map.get(streamOperator.getQueryId()).add(streamOperator);
					else{
						map.put(streamOperator.getQueryId(), new ArrayList<StreamOperator>());
						map.get(streamOperator.getQueryId()).add(streamOperator);
					}
				}
				
				for(String queryId : map.keySet()){
					List<List<String>> sets = generateSets(queryId, map);
					Collections.sort(sets, new OperatorSetComparator());
					List<Integer> childIdsForQuery = childIdsForQuery(queryId);
					
					if(childIdsForQuery.contains(childId)){
						for(List<String> set : sets){
							CanBeSentResult canBeSentResult = canBeSentToCollector(set, childId, resourceUsageDetails, queryId, currentCpuLoad, currentNwLoad);
							if(canBeSentResult.isCanBeSent()){
								System.out.println("SENDING "+set+" FROM "+getName()+" TO "+CloudSim.getEntityName(childId));
								
								double newCpuLoad = resourceUsageDetails.getCpuTrafficIntensity()*resourceUsageDetails.getMips()+canBeSentResult.getCpuLoad();
								resourceUsageDetails.setCpuTrafficIntensity(newCpuLoad/resourceUsageDetails.getMips());
								double newNwLoad = resourceUsageDetails.getNwTrafficIntensity()*resourceUsageDetails.getUplinkBandwidth()+canBeSentResult.getNwLoad();
								resourceUsageDetails.setNwTrafficIntensity(newNwLoad/resourceUsageDetails.getUplinkBandwidth());
								
								currentCpuLoad -= canBeSentResult.getCpuLoad();
								currentNwLoad -= canBeSentResult.getNwLoad();
								
								sendOperatorsToChild(queryId, set, childId);
								//System.out.println(CloudSim.clock()+" : "+CloudSim.getEntityName(childId));
								break;
							}
						}
					}
				}
				
			}
			childResourceUsages.clear();
		}
		
	}
	
	protected CanBeSentResult canBeSentToCollector(List<String> operators, int childDeviceId, ResourceUsageDetails resourceUsageDetails, String queryId
			, double currentCpuLoad, double currentNwLoad){
		for(String operator : operators){
			if(getChildToOperatorsMap().get(childDeviceId).contains(operator))
				return new CanBeSentResult(0, 0, false);
		}
		
		double cpuLoad = canBeSentToCpuCollector(operators, childDeviceId, resourceUsageDetails, queryId, currentCpuLoad, currentNwLoad);
		double nwLoad = canBeSentToNwCollector(operators, childDeviceId, resourceUsageDetails, queryId, currentCpuLoad, currentNwLoad);
		CanBeSentResult canBeSentResult = new CanBeSentResult();
		canBeSentResult.setCpuLoad(cpuLoad);
		canBeSentResult.setNwLoad(nwLoad);
		if(cpuLoad < 0 || nwLoad < 0){
			canBeSentResult.setCanBeSent(false);
		}else{
			canBeSentResult.setCanBeSent(true);
		}
			 
		return canBeSentResult;
	}
	
	/**
	 * If the set of operators can be sent from this device to child device with id=childDeviceId taking only NW into account
	 * @param operators set of operators in question
	 * @param childDeviceId ID of the child device(potential target)
	 * @param resourceUsageDetails resource statistics of the child device
	 * @param queryId 
	 * @param currentCpuLoad current CPU load on this device
	 * @param currentNwLoad current NW load on this device
	 * @return
	 */
	private double canBeSentToNwCollector(List<String> operators,
			int childDeviceId, ResourceUsageDetails resourceUsageDetails,
			String queryId, double currentCpuLoad, double currentNwLoad) {
		Map<String, Double> outputRateMap = new HashMap<String, Double>();
		StreamQuery streamQuery = getStreamQueryMap().get(queryId);
		
		//CORRECTED double changeInNwLoad = resourceUsageDetails.getNwTrafficIntensity()*resourceUsageDetails.getUplinkBandwidth();
		double changeInNwLoad = 0;
		
		List<String> leaves = getSubtreeLeaves(operators, queryId);
		for(String leaf : leaves){
			double outputRate = 0;
			for(String childOperator : streamQuery.getAllChildren(leaf)){
				if(streamQuery.isSensor(childOperator))
					changeInNwLoad -= getInputRateByChildOperatorAndNode(childOperator, childDeviceId)*streamQuery.getTupleNwLengthOfSensor(FogUtils.getSensorTypeFromSensorName(childOperator));
				else
					changeInNwLoad -= getInputRateByChildOperatorAndNode(childOperator, childDeviceId)*streamQuery.getOperatorByName(childOperator).getTupleFileLength();
				//CORRECTED outputRateMap.put(leaf, streamQuery.getSelectivity(leaf, childOperator)*getInputRateByChildOperatorAndNode(childOperator, childDeviceId));
				outputRate += streamQuery.getSelectivity(leaf, childOperator)*getInputRateByChildOperatorAndNode(childOperator, childDeviceId);
			}
			outputRateMap.put(leaf, outputRate);
		}
		
		boolean done = false;	// denotes whether output rate of all operators has been calculated
		while(!done){
			done = true;
			for(String operator : operators){
				if(!outputRateMap.containsKey(operator)){
					double outputRate = 0; boolean bool = true;
					for(String child : getSubtreeChildren(operator, operators, queryId)){
						if(!outputRateMap.containsKey(child)){
							bool = false;
							break;
						}
						outputRate += outputRateMap.get(child)*streamQuery.getSelectivity(operator, child);
					}
					if(bool){
						outputRateMap.put(operator, outputRate);
					}
					done = false;
				}
			}
		}
		
		for(String operator : getSubtreeApexes(operators, queryId)){
			changeInNwLoad += outputRateMap.get(operator)*streamQuery.getOperatorByName(operator).getTupleFileLength();
		}
		
		double finalNwLoadOnChild = resourceUsageDetails.getNwTrafficIntensity()*resourceUsageDetails.getUplinkBandwidth()
				+ changeInNwLoad;
		
		if(finalNwLoadOnChild/resourceUsageDetails.getUplinkBandwidth() > 1)
			return -1;
		
		/**
		 * Need to add code for deciding whether to send operators down or not based on cost of running them
		 */
		
		return 0;
	}

	/**
	 * If the set of operators can be sent from this device to child device with id=childDeviceId taking only CPU into account
	 * @param operators set of operators in question
	 * @param childDeviceId ID of the child device(potential target)
	 * @param resourceUsageDetails resource statistics of the child device
	 * @param queryId 
	 * @param currentCpuLoad current CPU load on this device
	 * @param currentNwLoad current NW load on this device
	 * @return
	 */
	protected double canBeSentToCpuCollector(List<String> operators, int childDeviceId, ResourceUsageDetails resourceUsageDetails, String queryId
			, double currentCpuLoad, double currentNwLoad){
		//System.out.println("Current CPU load = "+currentCpuLoad);
		StreamQuery streamQuery = getStreamQueryMap().get(queryId);
		double cpuLoad = 0;
		Map<String, Double> outputRateMap = new HashMap<String, Double>();
		
		List<String> leaves = getSubtreeLeaves(operators, queryId);
		for(String leaf : leaves){
			// calculate the output rate of each leaf operator in the subtree
			
			double outputRate = 0;
			for(String childOperator : streamQuery.getAllChildren(leaf)){
				if(streamQuery.isSensor(childOperator))
					cpuLoad += getInputRateByChildOperatorAndNode(childOperator, childDeviceId)*streamQuery.getTupleCpuLengthOfSensor(FogUtils.getSensorTypeFromSensorName(childOperator));
				else
					cpuLoad += getInputRateByChildOperatorAndNode(childOperator, childDeviceId)*streamQuery.getOperatorByName(childOperator).getTupleLength();
				//CORRECTED outputRateMap.put(leaf, streamQuery.getSelectivity(leaf, childOperator)*getInputRateByChildOperatorAndNode(childOperator, childDeviceId));
				outputRate += streamQuery.getSelectivity(leaf, childOperator)*getInputRateByChildOperatorAndNode(childOperator, childDeviceId);
			}
			//CORRECTED 
			outputRateMap.put(leaf, outputRate);
		}
		
		//now calculate the output rates of all the non-leaf operators in the subtree
		boolean done = false;	// denotes whether output rate of all operators has been calculated
		while(!done){
			done = true;
			for(String operator : operators){
				if(!outputRateMap.containsKey(operator)){
					double outputRate = 0, cpuLoadOperator = 0; boolean bool = true;
					for(String child : getSubtreeChildren(operator, operators, queryId)){
						if(!outputRateMap.containsKey(child)){
							bool = false;
							break;
						}
						outputRate += outputRateMap.get(child)*streamQuery.getSelectivity(operator, child);
						cpuLoadOperator += outputRateMap.get(child)*streamQuery.getOperatorByName(child).getTupleLength();
					}
					if(bool){
						outputRateMap.put(operator, outputRate);
						cpuLoad += cpuLoadOperator;
					}
					done = false;
				}
			}
		}
		
		// NOW, cpuLoad REPRESENTS THE LOAD ON CPU INCURRED BY operators IF THEY ARE MOVED TO childId
		
		//System.out.println(getName()+"\t"+CloudSim.getEntityName(childDeviceId)+"\t"+cpuLoad);
		
		double finalTrafficIntensityOnChild = (cpuLoad+resourceUsageDetails.getCpuTrafficIntensity()*resourceUsageDetails.getMips())/resourceUsageDetails.getMips(); 
		
		if(finalTrafficIntensityOnChild > 1)
			return -1;
		
		// NOW CALCULATING THE COST OF RUNNING THE SUBSET OF OPERATORS ON CHOSEN CHILD DEVICE
		
		List<List<String>> paths = getPathsInOperatorSubset(operators, queryId);
		
		double maxCostChildDevice = -1;
		for(List<String> path : paths){
			double pathCost = calculatePathCPUCostOnChildDevice(path, queryId, childDeviceId, 
					finalTrafficIntensityOnChild*resourceUsageDetails.getMips(), resourceUsageDetails.getMips());
			if(pathCost > maxCostChildDevice)
				maxCostChildDevice = pathCost;
		}
		
		// NOW CALCULATING THE COST OF RUNNING THE SUBSET OF OPERATORS ON CURRENT DEVICE
		double maxCostCurrentDevice = -1;
		for(List<String> path : paths){
			double pathCost = calculatePathCPUCostOnCurrentDevice(path, queryId, childDeviceId, 
					currentCpuLoad, getHost().getTotalMips());
			if(pathCost > maxCostCurrentDevice)
				maxCostCurrentDevice = pathCost;
		}
		
		System.out.println(CloudSim.clock()+"\tCost of running "+operators+" on "+getName()+" = "+maxCostCurrentDevice);
		System.out.println(CloudSim.clock()+"\tCost of running "+operators+" on "+CloudSim.getEntityName(childDeviceId)+" = "+maxCostChildDevice);
		
		if(maxCostChildDevice < maxCostCurrentDevice)
			return cpuLoad;
		else
			return -1;
		//return true;
	}


}
