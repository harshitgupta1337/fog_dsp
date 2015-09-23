package org.fog.utils;

public class FogEvents {
	private static final int BASE = 50;
	public static final int TUPLE_ARRIVAL = BASE + 1;
	public static final int LAUNCH_OPERATOR = BASE + 2;
	public static final int RELEASE_OPERATOR = BASE + 3;
	public static final int SENSOR_JOINED = BASE + 4;
	public static final int TUPLE_ACK = BASE + 5;
	public static final int QUERY_SUBMIT = BASE + 6;
	public static final int CALCULATE_INPUT_RATE = BASE + 7;
	public static final int CALCULATE_UTIL = BASE + 8;
	public static final int UPDATE_RESOURCE_USAGE = BASE + 9;
	public static final int UPDATE_TUPLE_QUEUE = BASE + 10;
	public static final int TUPLE_FINISHED = BASE + 11;
	public static final int ACTIVE_QUERY_UPDATE = BASE+12;
	public static final int CONTROLLER_RESOURCE_MANAGE = BASE+13;
	public static final int ADAPTIVE_OPERATOR_REPLACEMENT = BASE+14;
	public static final int GET_RESOURCE_USAGE = BASE+15;
	public static final int RESOURCE_USAGE = BASE+16;
}
