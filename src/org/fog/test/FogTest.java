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
			
			List<FogDevice> fogDevices = createFogDevices(queryId, broker.getId());

			
			StreamQuery query = createStreamQuery(queryId, broker.getId());
			
			Controller controller = new Controller("master-controller", fogDevices);
			controller.submitStreamQuery(query);
			
			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			CloudSim.stopSimulation();
			//System.out.println(CloudSim.clock());

			Log.printLine("CloudSimExample1 finished!");
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}
	}

	private static List<FogDevice> createFogDevices(String queryId, int userId) {
		final FogDevice gw0 = createFogDevice("gateway-0", new GeoCoverage(-100, 0, 0, 100));
		final FogDevice gw1 = createFogDevice("gateway-1", new GeoCoverage(0, 100, 0, 100));
		final FogDevice gw2 = createFogDevice("gateway-2", new GeoCoverage(-100, 0, -100, 0));
		final FogDevice gw3 = createFogDevice("gateway-3", new GeoCoverage(0, 100, -100, 0));
		
		final FogDevice l1_02 = createFogDevice("level1-02", new GeoCoverage(-100, 0, -100, 100));
		final FogDevice l1_13 = createFogDevice("level1-13", new GeoCoverage(0, 100, -100, 100));
		
		final FogDevice cloud = createFogDevice("cloud", new GeoCoverage(-FogUtils.MAX, FogUtils.MAX, -FogUtils.MAX, FogUtils.MAX));
		
		gw0.setParentId(l1_02.getId());
		gw2.setParentId(l1_02.getId());
		gw1.setParentId(l1_13.getId());
		gw3.setParentId(l1_13.getId());
		
		l1_02.setParentId(cloud.getId());
		l1_13.setParentId(cloud.getId());
		
		cloud.setParentId(-1);
		
		int transmitInterval = 100;
		
		Sensor sensor01 = new Sensor("sensor0-1", userId, queryId, gw0.getId(), null, transmitInterval);
		Sensor sensor02 = new Sensor("sensor0-2", userId, queryId, gw0.getId(), null, transmitInterval);
		
		Sensor sensor11 = new Sensor("sensor1-1", userId, queryId, gw1.getId(), null, transmitInterval);
		Sensor sensor12 = new Sensor("sensor1-2", userId, queryId, gw1.getId(), null, transmitInterval);
		
		Sensor sensor21 = new Sensor("sensor2-1", userId, queryId, gw2.getId(), null, transmitInterval);
		Sensor sensor22 = new Sensor("sensor2-2", userId, queryId, gw2.getId(), null, transmitInterval);
		
		Sensor sensor31 = new Sensor("sensor3-1", userId, queryId, gw3.getId(), null, transmitInterval);
		Sensor sensor32 = new Sensor("sensor3-2", userId, queryId, gw3.getId(), null, transmitInterval);

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
	private static FogDevice createFogDevice(String name, GeoCoverage geoCoverage) {

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips = 1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerOverbooking(mips))); // need to store Pe id and MIPS Rating
		//peList.add(new Pe(1, new PeProvisionerSimple(mips)));
		//peList.add(new Pe(2, new PeProvisionerSimple(mips)));
		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
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
		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$/Pe time unit).
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

		// 6. Finally, we need to create a PowerDatacenter object.
		FogDevice fogdevice = null;
		try {
			fogdevice = new FogDevice(name, geoCoverage, characteristics, new StreamOperatorAllocationPolicy(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return fogdevice;
	}
	
	private static StreamQuery createStreamQuery(String queryId, int userId){
		int mips = 1000;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		String vmm = "Xen"; // VMM name
		final StreamOperator spout = new StreamOperator(FogUtils.generateEntityId(), "spout", null, "sensor", queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 0.7);
		System.out.println(spout.getCurrentRequestedMips());
		final StreamOperator bolt = new StreamOperator(FogUtils.generateEntityId(), "bolt", null, "sensor", queryId, userId, mips, ram, bw, size, vmm, new TupleScheduler(mips, 1), 0.2);
		List<StreamOperator> operators = new ArrayList<StreamOperator>(){{add(spout); add(bolt);}};
		Map<String, String> edges = new HashMap<String, String>(){{put(spout.getName(), bolt.getName());}};
		GeoCoverage geoCoverage = new GeoCoverage(0, 100, -100, 100);
		StreamQuery query = new StreamQuery(queryId, operators, edges, geoCoverage);
		
		return query;
	}
}