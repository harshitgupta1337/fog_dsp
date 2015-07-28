package org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.fog.dsp.Controller;
import org.fog.dsp.StreamQuery;
import org.fog.entities.FogDevice;
import org.fog.entities.FogDeviceCharacteristics;
import org.fog.entities.Sensor;
import org.fog.entities.StreamOperator;
import org.fog.policy.StreamOperatorAllocationPolicy;
import org.fog.scheduler.StreamOperatorScheduler;
import org.fog.utils.FogUtils;
import org.fog.utils.GeoCoverage;
import org.fog.utils.GeoLocation;

/**
 * A simple example showing how to create a datacenter with one host and run one
 * cloudlet on it.
 */
public class FogSensorTest {

	public static void main(String[] args) {

		Log.printLine("Starting CloudSimExample1...");

		try {
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			CloudSim.init(num_user, calendar, trace_flag);

			final FogDevice gw = createFogDevice("gateway", new GeoCoverage(-10, 10, -10, 10));
			final FogDevice cloud = createFogDevice("cloud", new GeoCoverage(-20, 20, -20, 20));
			gw.setParentId(cloud.getId());

			String queryId = "query";
			
			Sensor sensor = new Sensor("sensor", FogUtils.USER_ID, queryId, gw.getId(), new GeoLocation(0, 0), 100);	
			
			StreamQuery query = createStreamQuery(queryId);
			
			List<FogDevice> fogDevices = new ArrayList<FogDevice>(){{add(gw);add(cloud);}};
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
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList.add(new Pe(2, new PeProvisionerSimple(mips)));
		// 4. Create Host with its id and list of PEs and add them to the list
		// of machines
		int hostId = FogUtils.generateEntityId();
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		Host host = new Host(
				hostId,
				new RamProvisionerSimple(ram),
				new BwProvisionerSimple(bw),
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
	
	private static StreamQuery createStreamQuery(String queryId){
		int mips = 1000;
		long size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		long bw = 1000;
		int pesNumber = 1; // number of cpus
		String vmm = "Xen"; // VMM name
		final StreamOperator spout = new StreamOperator(FogUtils.generateEntityId(), "spout", null, "sensor", queryId, FogUtils.USER_ID, mips, ram, bw, size, vmm, new CloudletSchedulerTimeShared(), 0.7);
		final StreamOperator bolt = new StreamOperator(FogUtils.generateEntityId(), "bolt", null, "sensor", queryId, FogUtils.USER_ID, mips, ram, bw, size, vmm, new CloudletSchedulerTimeShared(), 0.2);
		List<StreamOperator> operators = new ArrayList<StreamOperator>(){{add(spout); add(bolt);}};
		Map<String, String> edges = new HashMap<String, String>(){{put(spout.getName(), bolt.getName());}};
		GeoCoverage geoCoverage = new GeoCoverage(-5, 5, -5, 5);
		StreamQuery query = new StreamQuery(queryId, operators, edges, geoCoverage);
		
		return query;
	}
	
	

	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static DatacenterBroker createBroker() {
		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker("Broker");
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	/**
	 * Prints the Cloudlet objects.
	 *
	 * @param list list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent
				+ "Data center ID" + indent + "VM ID" + indent + "Time" + indent
				+ "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
				Log.print("SUCCESS");

				Log.printLine(indent + indent + cloudlet.getResourceId()
						+ indent + indent + indent + cloudlet.getVmId()
						+ indent + indent
						+ dft.format(cloudlet.getActualCPUTime()) + indent
						+ indent + dft.format(cloudlet.getExecStartTime())
						+ indent + indent
						+ dft.format(cloudlet.getFinishTime()));
			}
		}
	}

}