package org.fog.test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.sdn.overbooking.BwProvisionerOverbooking;
import org.cloudbus.cloudsim.sdn.overbooking.PeProvisionerOverbooking;
import org.fog.dsp.Controller;
import org.fog.dsp.StreamQuery;
import org.fog.entities.FogBroker;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Sensor;
import org.fog.entities.StreamOperator;
import org.fog.policy.StreamOperatorAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.scheduler.TupleScheduler;
import org.fog.utils.FogUtils;
import org.fog.utils.GeoCoverage;
import org.fog.utils.OperatorEdge;

public class FogTest2Sensors3Ops {

	static int sensorTupleCpuSize = 1;
	static int sensorTupleNwSize = 1000;
	public static void main(String[] args) {

		Log.printLine("Starting FogTest...");

		try {
			Log.disable();
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			CloudSim.init(num_user, calendar, trace_flag);

			String queryId = "query";
			
			FogBroker broker = new FogBroker("broker");
			
			int transmitInterval = 50;

			StreamQuery query = createStreamQuery(queryId, broker.getId(), transmitInterval);

			List<FogDevice> fogDevices = createFogDevices(queryId, broker.getId(), transmitInterval);
			
			for(int i=0;i<1;i++){
				createSensor("sensor-TYPE-"+i, query, broker.getId(), CloudSim.getEntityId("gateway-0"), transmitInterval, sensorTupleCpuSize, sensorTupleNwSize, "TYPE", "spout");
			}
			for(int i=0;i<1;i++){
				createSensor("sensor-TYPE1-"+i, query, broker.getId(), CloudSim.getEntityId("gateway-0"), transmitInterval, sensorTupleCpuSize, sensorTupleNwSize*2, "TYPE1", "spout1");
			}

			Controller controller = new Controller("master-controller", fogDevices);
			System.out.println("Yo");
			controller.submitStreamQuery(query);
			System.out.println("YOYO");
			
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	private static void createSensor(String sensorName, StreamQuery query, int userId, int parentId, int transmitInterval, int tupleCpuSize, int tupleNwSize, String type, String spoutName){
		Sensor sensor0 = new Sensor(sensorName, userId, query.getQueryId(), parentId, null, transmitInterval, tupleCpuSize, tupleNwSize, type, spoutName);
		query.registerSensor(sensor0);
		
	}
	
	private static List<FogDevice> createFogDevices(String queryId, int userId, int transmitInterval) {
		final FogDevice gw0 = createFogDevice("gateway-0", 100, new GeoCoverage(-100, 100, -100, 100), 1000, 0);
		
		final FogDevice cloud = createFogDevice("cloud", FogUtils.MAX, new GeoCoverage(-FogUtils.MAX, FogUtils.MAX, -FogUtils.MAX, FogUtils.MAX), FogUtils.MAX, 0);
		
		gw0.setParentId(cloud.getId());
		cloud.setParentId(-1);

		List<FogDevice> fogDevices = new ArrayList<FogDevice>(){{add(gw0);add(cloud);}};
		return fogDevices;
	}

	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static FogDevice createFogDevice(String name, int mips, GeoCoverage geoCoverage, double uplinkBandwidth, double latency) {

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating

		int hostId = FogUtils.generateEntityId();
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		Host host = new Host(
				hostId,
				new RamProvisionerSimple(ram),
				new BwProvisionerOverbooking(bw),
				storage,
				peList,
				new StreamOperatorScheduler(peList)
			);

		List<Host> hostList = new ArrayList<Host>();
		hostList.add(host);

		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
										// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
													// devices by now

		FogDeviceCharacteristics characteristics = new FogDeviceCharacteristics(
				arch, os, vmm, host, time_zone, cost, costPerMem,
				costPerStorage, costPerBw, geoCoverage);

		FogDevice fogdevice = null;
		try {
			fogdevice = new FogDevice(name, geoCoverage, characteristics, new StreamOperatorAllocationPolicy(hostList), storageList, 0, uplinkBandwidth, latency);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fogdevice;
	}
	
	private static StreamQuery createStreamQuery(String queryId, int userId, int transmitInterval){
		int mips = 1000;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		String vmm = "Xen"; // VMM name
		final StreamOperator spout = new StreamOperator(FogUtils.generateEntityId(), "spout", null, "TYPE", 
				queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 1, 1, 1, 100, 
				2/((double)transmitInterval));
		final StreamOperator spout1 = new StreamOperator(FogUtils.generateEntityId(), "spout1", null, "TYPE1", 
				queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 1, 1, 1, 200, 
				2/((double)transmitInterval));
		final StreamOperator bolt = new StreamOperator(FogUtils.generateEntityId(), "bolt", null, null, queryId, 
				userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 1, 1, 1, 100, 0);
		List<StreamOperator> operators = new ArrayList<StreamOperator>(){{add(spout);add(spout1);add(bolt);}};
		Map<String, String> edges = new HashMap<String, String>(){{put("spout", "bolt");put("spout1", "bolt");}};
		GeoCoverage geoCoverage = new GeoCoverage(-100, 100, -100, 100);
		List<OperatorEdge> operatorEdges = new ArrayList<OperatorEdge>(){{add(new OperatorEdge("sensor-TYPE-", "spout", 1));add(new OperatorEdge("sensor-TYPE1-", "spout1", 3));add(new OperatorEdge("spout", "bolt", 0.2));add(new OperatorEdge("spout1", "bolt", 0.3));}};
		StreamQuery query = new StreamQuery(queryId, operators, edges, geoCoverage, operatorEdges);
		return query;
	}
}