package org.fog.dsp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.fog.entities.FogDevice;
import org.fog.entities.StreamOperator;

public class OperatorPlacementNoOverbooking {

	private List<FogDevice> fogDevices;
	private StreamQuery streamQuery;
	private Map<String, Integer> operatorToDeviceMap;
	private FogDevice lowestDevice;
	
	public OperatorPlacementNoOverbooking(List<FogDevice> fogDevices, StreamQuery streamQuery){
		this.setFogDevices(fogDevices);
		this.setStreamQuery(streamQuery);
		
		List<String> leafOperators = streamQuery.getLeaves();
		this.operatorToDeviceMap = new HashMap<String, Integer>();
		for(String leafOperator : leafOperators){
			StreamOperator operator = streamQuery.getOperatorByName(leafOperator);
			
			FogDevice currentDevice = getLowestCoveringFogDevice(fogDevices, streamQuery);
			this.lowestDevice = currentDevice;
			// begin placing the operators in some order
			
			while(true){
				if(currentDevice != null){
					if(canBeCreated(currentDevice, operator)){
						operatorToDeviceMap.put(operator.getName(), currentDevice.getId());
						break;
					}else{
						currentDevice = getDeviceById(currentDevice.getParentId());
					}
				}else{
					break;
				}
			}
		}
		
		List<String> remainingOperators = new ArrayList<String>();
		
		for(StreamOperator operator : streamQuery.getOperators()){
			if(!operatorToDeviceMap.containsKey(operator.getName()))
				remainingOperators.add(operator.getName());
		}
		
		while(remainingOperators.size() > 0){
			//System.out.println("BOOO");
			String operator = remainingOperators.get(0);
			if(allChildrenMapped(operator)){
				FogDevice currentDevice = getLowestSuitableDevice(operator);
				while(true){
					if(currentDevice != null){
						if(canBeCreated(currentDevice, streamQuery.getOperatorByName(operator))){
							operatorToDeviceMap.put(operator, currentDevice.getId());
							System.out.println(operator + " placed");
							remainingOperators.remove(0);
							break;
						}else{
							//System.out.println("ASASASAS");
							currentDevice = getDeviceById(currentDevice.getParentId());
						}
					}else{
						break;
					}
				}
				
			}
		}
	}
	
	private FogDevice getLowestSuitableDevice(String operator){
		List<String> children = streamQuery.getAllChildren(operator);
		FogDevice highestChildrenHolder = getLowestDevice();
		for(String child : children){
			if(getDeviceById(operatorToDeviceMap.get(child)).getGeoCoverage().covers(highestChildrenHolder.getGeoCoverage()))
				highestChildrenHolder = getDeviceById(operatorToDeviceMap.get(child));
		}
		return highestChildrenHolder;
	}
	
	private boolean allChildrenMapped(String operator){
		boolean result = true;
		List<String> children = streamQuery.getAllChildren(operator);
		for(String child : children){
			if(!this.operatorToDeviceMap.containsKey(child)){
				result = false;
			}
		}
		return result;
	}
	
	private FogDevice getDeviceById(int id){
		for(FogDevice dev : fogDevices){
			if(dev.getId() == id)
				return dev;
		}
		return null;
	}
	
	private double getTotalMipsRequestedOnFogDevice(FogDevice fogDevice){
		double totalMips = 0;
		for(String operator : operatorToDeviceMap.keySet()){
			if(operatorToDeviceMap.get(operator) == fogDevice.getId()){
				totalMips += streamQuery.getOperatorByName(operator).getCurrentRequestedTotalMips();
			}
		}
		return totalMips;
	}
	
	private boolean canBeCreated(FogDevice fogDevice, StreamOperator streamOperator){
		//System.out.println("-----------------------------------------------");
		//System.out.println(streamOperator.getName()+"\trequests\t"+streamOperator.getCurrentRequestedTotalMips()+" mips");
		//System.out.println(fogDevice.getName()+"\tHAS \t"+fogDevice.getHost().getTotalMips());
		//System.out.println("-----------------------------------------------");
		
		if(streamOperator.getCurrentRequestedTotalMips() + getTotalMipsRequestedOnFogDevice(fogDevice) <= fogDevice.getHost().getTotalMips())
			return fogDevice.getVmAllocationPolicy().allocateHostForVm(streamOperator);
		else
			return false;
	}
	
	private FogDevice getLowestCoveringFogDevice(List<FogDevice> fogDevices, StreamQuery streamQuery){
		FogDevice coverer = null;
		for(FogDevice fogDevice : fogDevices){
			if(fogDevice.getGeoCoverage().covers(streamQuery.getGeoCoverage())){
				if(coverer == null)
					coverer = fogDevice;
				else if(coverer.getGeoCoverage().covers(fogDevice.getGeoCoverage()))
					coverer = fogDevice;
			}
		}
		return coverer;
	}

	public List<FogDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}

	public StreamQuery getStreamQuery() {
		return streamQuery;
	}

	public void setStreamQuery(StreamQuery streamQuery) {
		this.streamQuery = streamQuery;
	}

	public FogDevice getLowestDevice() {
		return lowestDevice;
	}

	public void setLowestDevice(FogDevice lowestDevice) {
		this.lowestDevice = lowestDevice;
	}

	public Map<String, Integer> getOperatorToDeviceMap() {
		return operatorToDeviceMap;
	}

	public void setOperatorToDeviceMap(Map<String, Integer> operatorToDeviceMap) {
		this.operatorToDeviceMap = operatorToDeviceMap;
	}
}
