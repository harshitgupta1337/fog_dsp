package org.fog.scheduler;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.ResCloudlet;

public class TupleScheduler extends CloudletScheduler{

	@Override
	public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double cloudletSubmit(Cloudlet gl, double fileTransferTime) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double cloudletSubmit(Cloudlet gl) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Cloudlet cloudletCancel(int clId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean cloudletPause(int clId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public double cloudletResume(int clId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void cloudletFinish(ResCloudlet rcl) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getCloudletStatus(int clId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isFinishedCloudlets() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Cloudlet getNextFinishedCloudlet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int runningCloudlets() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Cloudlet migrateCloudlet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getTotalUtilizationOfCpu(double time) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Double> getCurrentRequestedMips() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getTotalCurrentAvailableMipsForCloudlet(ResCloudlet rcl,
			List<Double> mipsShare) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getTotalCurrentRequestedMipsForCloudlet(ResCloudlet rcl,
			double time) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getTotalCurrentAllocatedMipsForCloudlet(ResCloudlet rcl,
			double time) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getCurrentRequestedUtilizationOfRam() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getCurrentRequestedUtilizationOfBw() {
		// TODO Auto-generated method stub
		return 0;
	}

}
