package com.iotdbResultCSV.dom;

public class insertBean {

	private int id;
	private String recordTime;
	private String clientName;
	private String operation;
	private int okPoint;
	private int failPoint;
	private double latency;
	private double rate;
	private String remark;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getRecordTime() {
		return recordTime;
	}
	public void setRecordTime(String recordTime) {
		this.recordTime = recordTime;
	}
	public String getClientName() {
		return clientName;
	}
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public int getOkPoint() {
		return okPoint;
	}
	public void setOkPoint(int okPoint) {
		this.okPoint = okPoint;
	}
	public int getFailPoint() {
		return failPoint;
	}
	public void setFailPoint(int failPoint) {
		this.failPoint = failPoint;
	}
	public double getLatency() {
		return latency;
	}
	public void setLatency(double latency) {
		this.latency = latency;
	}
	public double getRate() {
		return rate;
	}
	public void setRate(double rate) {
		this.rate = rate;
	}
	public String getRemark() {
		return remark;
	}
	public void setRemark(String remark) {
		this.remark = remark;
	}
}
