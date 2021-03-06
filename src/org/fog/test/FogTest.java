package org.fog.test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.HostDynamicWorkload;
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

public class FogTest {

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
			
			int transmitInterval = 10;
			
			List<FogDevice> fogDevices = createFogDevices(queryId, broker.getId(), transmitInterval);

			
			StreamQuery query = createStreamQuery(queryId, broker.getId(), transmitInterval);
			
			Controller controller = new Controller("master-controller", fogDevices);
			controller.submitStreamQuery(query);
			
			CloudSim.startSimulation();

			CloudSim.stopSimulation();

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	private static List<FogDevice> createFogDevices(String queryId, int userId, int transmitInterval) {
		final FogDevice gw0 = createFogDevice("gateway-0", 1000, new GeoCoverage(-100, 0, 0, 100), 1000, 1);
		final FogDevice gw1 = createFogDevice("gateway-1", 1000, new GeoCoverage(0, 100, 0, 100), 1000, 1);
		final FogDevice gw2 = createFogDevice("gateway-2", 1000, new GeoCoverage(-100, 0, -100, 0), 1000, 1);
		final FogDevice gw3 = createFogDevice("gateway-3", 1000, new GeoCoverage(0, 100, -100, 0), 1000, 1);
		
		final FogDevice l1_02 = createFogDevice("level1-02", 1000, new GeoCoverage(-100, 0, -100, 100), 100, 1);
		final FogDevice l1_13 = createFogDevice("level1-13", 1000, new GeoCoverage(0, 100, -100, 100), 10, 1);
		
		final FogDevice cloud = createFogDevice("cloud", 10000, new GeoCoverage(-FogUtils.MAX, FogUtils.MAX, -FogUtils.MAX, FogUtils.MAX), 0.01, 10);
		
		gw0.setParentId(l1_02.getId());
		gw2.setParentId(l1_02.getId());
		gw1.setParentId(l1_13.getId());
		gw3.setParentId(l1_13.getId());
		
		l1_02.setParentId(cloud.getId());
		l1_13.setParentId(cloud.getId());
		
		cloud.setParentId(-1);
		
		int tupleCpuSize = 1000;
		int tupleNwSize = 1000;
				
		Sensor sensor01 = new Sensor("sensor0-1", userId, queryId, gw0.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		Sensor sensor02 = new Sensor("sensor0-2", userId, queryId, gw0.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		
		Sensor sensor11 = new Sensor("sensor1-1", userId, queryId, gw1.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		Sensor sensor12 = new Sensor("sensor1-2", userId, queryId, gw1.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		
		Sensor sensor21 = new Sensor("sensor2-1", userId, queryId, gw2.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		Sensor sensor22 = new Sensor("sensor2-2", userId, queryId, gw2.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		
		Sensor sensor31 = new Sensor("sensor3-1", userId, queryId, gw3.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);
		Sensor sensor32 = new Sensor("sensor3-2", userId, queryId, gw3.getId(), null, transmitInterval, tupleCpuSize, tupleNwSize);

		List<FogDevice> fogDevices = new ArrayList<FogDevice>(){{add(gw0);add(gw1);add(gw2);add(gw3);add(l1_02);add(l1_13);add(cloud);}};
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
		final StreamOperator spout = new StreamOperator(FogUtils.generateEntityId(), "spout", null, "sensor", queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 1, 1, 100, 100, 4/((double)transmitInterval));
		final StreamOperator bolt = new StreamOperator(FogUtils.generateEntityId(), "bolt", null, "sensor", queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 0.002, 0.002, 100, 100, 4/((double)transmitInterval));
		List<StreamOperator> operators = new ArrayList<StreamOperator>(){{add(spout); add(bolt);}};
		Map<String, String> edges = new HashMap<String, String>(){{put(spout.getName(), bolt.getName());}};
		GeoCoverage geoCoverage = new GeoCoverage(0, 100, -100, 100);
		List<OperatorEdge> operatorEdges = new ArrayList<OperatorEdge>(){{add(new OperatorEdge("sensor", "spout", 0.1)); add(new OperatorEdge(spout.getName(), bolt.getName(), 0.5));}};
		StreamQuery query = new StreamQuery(queryId, operators, edges, geoCoverage, operatorEdges);
		
		return query;
	}
}