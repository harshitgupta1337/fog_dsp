package org.fog.utils;

import java.util.HashMap;
import java.util.Map;

public class TupleEmitTimes {

	private Map<Integer, Double> startTimeMap;
	
	private static TupleEmitTimes instance;
	
	public static TupleEmitTimes getInstance(){
		if(instance==null)
			instance = new TupleEmitTimes();
		return instance;
	}
	
	public static void removeEmitTime(int tupleId){
		getInstance().getStartTimeMap().remove(tupleId);
	}
	
	public static void setEmitTime(int tupleId, double time){
		getInstance().getStartTimeMap().put(tupleId, time);
	}
	
	public static double getEmitTime(int tupleId){
		return getInstance().getStartTimeMap().get(tupleId);
	}
	
	private TupleEmitTimes(){
		this.setStartTimeMap(new HashMap<Integer, Double>());
	}

	public Map<Integer, Double> getStartTimeMap() {
		return startTimeMap;
	}

	public void setStartTimeMap(Map<Integer, Double> startTimeMap) {
		this.startTimeMap = startTimeMap;
	}
	
	
}
