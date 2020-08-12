package com.iotdbResultCSV.function;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.iotdbResultCSV.dom.analyzeResultBean;
import com.iotdbResultCSV.dom.insertBean;
import com.iotdbResultCSV.dom.latencyrateRecordBean;

public class recordCSV {

	private String dblink;
	private String user;
	private String password;
	private String dbdriver;
	PreparedStatement ps = null;
	Connection ct = null;
	ResultSet rs = null;
	Statement  stmt = null;
	
	/**
	 * 构造函数
	 * @param dbip
	 * @param dbName
	 * @param user
	 * @param password
	 */
	public recordCSV(String dbip,String dbName,String user,String password){
		this.dblink = "jdbc:mysql://"+dbip+":3306/"+dbName+"";
		this.user = user;
		this.password = password;
		this.dbdriver = "com.mysql.jdbc.Driver";
	}
	
	
	/**
	 * 写人插入或八个查询的默认结果记录
	 * @param recordPath
	 * @param fileName
	 * @param alResult
	 */
	public void recordCSV(String recordPath,String fileName,ArrayList alResult){
		FileWriter fw = null;
		int flag = 0;
		Date dt= new Date();
		Long start = Long.valueOf(dt.getTime());
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
		String time = sdFormatter.format(start);
try {
			
			File f = new File(recordPath+""+fileName+"_"+time+".csv");
			if(!f.exists()){
				f.createNewFile();
			}

				fw = new FileWriter(f,true);
				fw.write("id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark");
				fw.write("\n");
				for(int i=0;i<alResult.size();i++){
					insertBean ib = (insertBean)alResult.get(i);
					fw.write(ib.getId()+","+ib.getRecordTime()+","+ib.getClientName()+","+ib.getOperation()+","+ib.getOkPoint()+","+ib.getFailPoint()+","+ib.getLatency()+","+ib.getRate()+","+ib.getRemark() );
					fw.write("\n");
				}
			} catch (Exception erecordLog) {
			erecordLog.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}
	
	/* 初始化分析结果CSV的表头 （未使用）
	public void initAnalyzeCSV(String recordPath,String fileName){
		FileWriter fw = null;
		Date dt= new Date();
		Long start = Long.valueOf(dt.getTime());//_HH_mm_ss
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy_MM_dd");
		String time = sdFormatter.format(start);
		try {
			File f = new File(recordPath+"Analyze_"+time+".csv");
			if(!f.exists()){
				f.createNewFile();
			}
			fw = new FileWriter(f,true);
			fw.write("\n");
			fw.write("operation,okOperationNum,okPointNum,failOperationNum,failPointNum,throughput,AVG,MIN,P10,P25,MEDIAN,P75,P90,P95,P99,P999,MAX,SLOWEST_THREAD");
			
					
				
		} catch (Exception erecordAnalyzeCSV) {
			erecordAnalyzeCSV.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}
	*/
	
	/**
	 * 根据操作类型（insert、Q1 - 8 ）分类检索final_result的内容并写入CSV
	 * @param recordPath
	 * @param fileName
	 * @param alResult
	 * 
	 */
	public void recordAnalyzeCSV(String recordPath,String fileName,ArrayList alResult,String operator){
		FileWriter fw = null;
		Date dt= new Date();
		Long start = Long.valueOf(dt.getTime());//_HH_mm_ss
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy_MM_dd");
		String time = sdFormatter.format(start);
		
		//写入吞吐量
		try {
			File f = new File(recordPath+"Analyze_report.csv");
			if(!f.exists()){
				f.createNewFile();
			}
			fw = new FileWriter(f,true);
//				按字段写值，不支持循环操作。
				fw.write(operator+",");//本行的操作名称
				analyzeResultBean arb0 = (analyzeResultBean)alResult.get(0);
				fw.write(arb0.getResult_value()+",");
				analyzeResultBean arb1 = (analyzeResultBean)alResult.get(1);
				fw.write(arb1.getResult_value()+",");
				analyzeResultBean arb2 = (analyzeResultBean)alResult.get(2);
				fw.write(arb2.getResult_value()+",");
				analyzeResultBean arb3 = (analyzeResultBean)alResult.get(3);
				fw.write(arb3.getResult_value()+",");
				analyzeResultBean arb4 = (analyzeResultBean)alResult.get(4);
				fw.write(arb4.getResult_value()+",");
				analyzeResultBean arb5 = (analyzeResultBean)alResult.get(5);
				fw.write(arb5.getResult_value()+",");
				analyzeResultBean arb6 = (analyzeResultBean)alResult.get(6);
				fw.write(arb6.getResult_value()+",");
				analyzeResultBean arb7 = (analyzeResultBean)alResult.get(7);
				fw.write(arb7.getResult_value()+",");
				analyzeResultBean arb8 = (analyzeResultBean)alResult.get(8);
				fw.write(arb8.getResult_value()+",");
				analyzeResultBean arb9 = (analyzeResultBean)alResult.get(9);
				fw.write(arb9.getResult_value()+",");
				analyzeResultBean arb10 = (analyzeResultBean)alResult.get(10);
				fw.write(arb10.getResult_value()+",");
				analyzeResultBean arb11 = (analyzeResultBean)alResult.get(11);
				fw.write(arb11.getResult_value()+",");
				analyzeResultBean arb12 = (analyzeResultBean)alResult.get(12);
				fw.write(arb12.getResult_value()+",");
				analyzeResultBean arb13 = (analyzeResultBean)alResult.get(13);
				fw.write(arb13.getResult_value()+",");
				analyzeResultBean arb14 = (analyzeResultBean)alResult.get(14);
				fw.write(arb14.getResult_value()+",");
				analyzeResultBean arb15 = (analyzeResultBean)alResult.get(15);
				fw.write(arb15.getResult_value()+",");
				analyzeResultBean arb16 = (analyzeResultBean)alResult.get(16);
				fw.write(arb16.getResult_value()+",");
				fw.write("\n");
				
		} catch (Exception erecordAnalyzeCSV) {
			erecordAnalyzeCSV.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}
	
	//创建直方图源数据文件,直接从final_result表查找
	public void createHistogramSource(String recordPath,String fileName,ArrayList alResult,String operator){
		FileWriter fw = null;
		
		try {
			File f = new File(recordPath+"histogram.dat");
			if(!f.exists()){
				f.createNewFile();
			}
			fw = new FileWriter(f,true);
			
			
//				按字段写值，不支持循环操作。
				analyzeResultBean arb0 = (analyzeResultBean)alResult.get(0);
				fw.write(arb0.getResult_value()+",");
				analyzeResultBean arb1 = (analyzeResultBean)alResult.get(1);
				fw.write(arb1.getResult_value()+",");
				analyzeResultBean arb2 = (analyzeResultBean)alResult.get(2);
				fw.write(arb2.getResult_value()+",");
				analyzeResultBean arb3 = (analyzeResultBean)alResult.get(3);
				fw.write(arb3.getResult_value()+",");
				analyzeResultBean arb4 = (analyzeResultBean)alResult.get(4);
				fw.write(arb4.getResult_value()+",");
				analyzeResultBean arb5 = (analyzeResultBean)alResult.get(5);
				fw.write(arb5.getResult_value()+",");
				analyzeResultBean arb6 = (analyzeResultBean)alResult.get(6);
				fw.write(arb6.getResult_value()+",");
				analyzeResultBean arb7 = (analyzeResultBean)alResult.get(7);
				fw.write(arb7.getResult_value()+",");
				analyzeResultBean arb8 = (analyzeResultBean)alResult.get(8);
				fw.write(arb8.getResult_value()+",");
				analyzeResultBean arb9 = (analyzeResultBean)alResult.get(9);
				fw.write(arb9.getResult_value()+",");
				analyzeResultBean arb10 = (analyzeResultBean)alResult.get(10);
				fw.write(arb10.getResult_value()+",");
				analyzeResultBean arb11 = (analyzeResultBean)alResult.get(11);
				fw.write(arb11.getResult_value()+",");
				analyzeResultBean arb12 = (analyzeResultBean)alResult.get(12);
				fw.write(arb12.getResult_value()+",");
				analyzeResultBean arb13 = (analyzeResultBean)alResult.get(13);
				fw.write(arb13.getResult_value()+",");
				analyzeResultBean arb14 = (analyzeResultBean)alResult.get(14);
				fw.write(arb14.getResult_value()+",");
				analyzeResultBean arb15 = (analyzeResultBean)alResult.get(15);
				fw.write(arb15.getResult_value()+",");
				analyzeResultBean arb16 = (analyzeResultBean)alResult.get(16);
				fw.write(arb16.getResult_value()+",");
				fw.write("\n");
				
		} catch (Exception erecordAnalyzeCSV) {
			erecordAnalyzeCSV.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}
	
	
	//记录每个查询场景的latency和rate
	public void recordLatencyRateCSV(String recordPath,String[] mp,String operator,String testdate){
		FileWriter fw = null;
		String rLatency = "";
		String rRate = "";
		rLatency = mp[0];
		rRate = mp[1];
		
		try {
			File f = new File(recordPath+"Latency_Rate_report.csv");
			if(!f.exists()){
				f.createNewFile();
			}
			fw = new FileWriter(f,true);
			fw.write(operator+"\n");
			fw.write("TestDate: "+testdate+"    ");
			fw.write("latency : "+mp[0]+"    ");
			fw.write("rate : "+mp[1]+"    ");
			fw.write("\n");
		} catch (Exception erecordAnalyzeCSV) {
			erecordAnalyzeCSV.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}

	//重新实现latency和rate的记录方法
	public void recordLatencyRateCSVbyList(String recordPath,ArrayList recordInfo){
		//将所有需要记录的内容通过list传入
		FileWriter fw = null;
		int count = recordInfo.size();//得到结果集长度
		String[] operationList = new String[27];//初始化4个字符数组，分别存储operational，date,lantency，rate
		String[] dateList = new String[27];
		String[] latencyList = new String[27];
		String[] rateList  = new String[27];
		for(int i=0;i<count;i++){
			latencyrateRecordBean lrrb = (latencyrateRecordBean)recordInfo.get(i);
			dateList[i] = lrrb.getTime();
			latencyList[i] = lrrb.getLatency();
			rateList[i] = lrrb.getTime();
			operationList[i] = lrrb.getOperator();
		}
		try {
			File f = new File(recordPath+"Latency_Rate_report.csv");
			if(!f.exists()){
				f.createNewFile();
			}
			fw = new FileWriter(f,true);
			fw.write("operational,TestDate,latency, rate"+"\n");
			fw.write(operationList[0]+",,,"+"\n");
			fw.write(","+dateList[0]+","+latencyList[0]+","+rateList[0]+"\n");
			fw.write(","+dateList[1]+","+latencyList[1]+","+rateList[1]+"\n");
			fw.write(","+dateList[2]+","+latencyList[2]+","+rateList[2]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[3]+",,,"+"\n");
			fw.write(","+dateList[3]+","+latencyList[3]+","+rateList[3]+"\n");
			fw.write(","+dateList[4]+","+latencyList[4]+","+rateList[4]+"\n");
			fw.write(","+dateList[5]+","+latencyList[5]+","+rateList[5]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[6]+",,,"+"\n");
			fw.write(","+dateList[6]+","+latencyList[6]+","+rateList[6]+"\n");
			fw.write(","+dateList[7]+","+latencyList[7]+","+rateList[7]+"\n");
			fw.write(","+dateList[8]+","+latencyList[8]+","+rateList[8]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[9]+",,,"+"\n");
			fw.write(","+dateList[9]+","+latencyList[9]+","+rateList[9]+"\n");
			fw.write(","+dateList[10]+","+latencyList[10]+","+rateList[10]+"\n");
			fw.write(","+dateList[11]+","+latencyList[11]+","+rateList[11]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[12]+",,,"+"\n");
			fw.write(","+dateList[12]+","+latencyList[12]+","+rateList[12]+"\n");
			fw.write(","+dateList[13]+","+latencyList[13]+","+rateList[13]+"\n");
			fw.write(","+dateList[14]+","+latencyList[14]+","+rateList[14]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[15]+",,,"+"\n");
			fw.write(","+dateList[15]+","+latencyList[15]+","+rateList[15]+"\n");
			fw.write(","+dateList[16]+","+latencyList[16]+","+rateList[16]+"\n");
			fw.write(","+dateList[17]+","+latencyList[17]+","+rateList[17]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[18]+",,,"+"\n");
			fw.write(","+dateList[18]+","+latencyList[18]+","+rateList[18]+"\n");
			fw.write(","+dateList[19]+","+latencyList[19]+","+rateList[19]+"\n");
			fw.write(","+dateList[20]+","+latencyList[20]+","+rateList[20]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[21]+",,,"+"\n");
			fw.write(","+dateList[21]+","+latencyList[21]+","+rateList[21]+"\n");
			fw.write(","+dateList[22]+","+latencyList[22]+","+rateList[22]+"\n");
			fw.write(","+dateList[23]+","+latencyList[23]+","+rateList[23]+"\n");
			fw.write(",,,"+"\n");
			fw.write(operationList[24]+",,,"+"\n");
			fw.write(","+dateList[24]+","+latencyList[24]+","+rateList[24]+"\n");
			fw.write(","+dateList[25]+","+latencyList[25]+","+rateList[25]+"\n");
			fw.write(","+dateList[26]+","+latencyList[26]+","+rateList[26]+"\n");
			fw.write(",,,"+"\n");

		} catch (Exception erecordAnalyzeCSV) {
			erecordAnalyzeCSV.printStackTrace();
		}
		finally{
			if(fw != null){
				try{
					fw.close();
				}catch (IOException e){
					throw new  RuntimeException("Close Failed");
				}
			}
		}
	}

}
