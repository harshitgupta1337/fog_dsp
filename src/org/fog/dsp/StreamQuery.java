package org.fog.dsp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.fog.entities.StreamOperator;
import org.fog.utils.GeoCoverage;

public class StreamQuery {

	private List<StreamOperator> operators;
	private Map<String, String> edges;
	private GeoCoverage geoCoverage;
	private String queryId;
	
	public StreamQuery(String queryId, List<StreamOperator> operators, Map<String, String> edges, GeoCoverage geoCoverage){
		this.operators = operators;
		this.edges = edges;
		this.geoCoverage = geoCoverage;
		setQueryId(queryId);
	}
	
	public StreamOperator getOperatorByName(String name){
		for(StreamOperator op : operators){
			if(op.getName().equals(name))
				return op;
		}
		return null;
	}

	public StreamOperator getNextOperator(String name){
		return getOperatorByName(edges.get(name));
	}
	
	public List<String> getLeaves(){
		List<String> leaves = new ArrayList<String>();
		for(StreamOperator op : operators){
			String opName = op.getName();
			if(!edges.containsValue(opName)){
				leaves.add(opName);
			}
		}
		return leaves;
	}
	
	public List<String> getAllChildren(String name){
		List<String> children = new ArrayList<String>();
		for(String operatorName : edges.keySet()){
			if(edges.get(operatorName).equals(name))
				children.add(operatorName);
		}
		return children;
	}
	
	public List<StreamOperator> getOperators() {
		return operators;
	}
	
	public void setOperators(List<StreamOperator> operators) {
		this.operators = operators;
	}

	public Map<String, String> getEdges() {
		return edges;
	}

	public void setEdges(Map<String, String> edges) {
		this.edges = edges;
	}

	public GeoCoverage getGeoCoverage() {
		return geoCoverage;
	}

	public void setGeoCoverage(GeoCoverage geoCoverage) {
		this.geoCoverage = geoCoverage;
	}

	public String getQueryId() {
		return queryId;
	}

	public void setQueryId(String queryId) {
		this.queryId = queryId;
	}
	
	
}
