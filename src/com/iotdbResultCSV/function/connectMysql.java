package com.iotdbResultCSV.function;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import com.iotdbResultCSV.dom.analyzeResultBean;
import com.iotdbResultCSV.dom.insertBean;
import com.iotdbResultCSV.dom.insertResultBean;
import com.iotdbResultCSV.dom.throughputBean;


public class connectMysql {
	private String dblink;
	private String user;
	private String password;
	private String dbdriver;
	PreparedStatement ps = null;
	Connection ct = null;
	ResultSet rs = null;
	Statement  stmt = null;

	
	/**
	 * 构造函数，加载数据库连接信息
	 * @param dbip
	 * @param dbName
	 * @param user
	 * @param password
	 */
	public connectMysql(String dbip,String dbName,String user,String password ){
		this.dblink = "jdbc:mysql://"+dbip+":3306/"+dbName+"";
		this.user = user;
		this.password = password;
		this.dbdriver = "com.mysql.jdbc.Driver";
		
	}

	
	/**
	 * 查询最新执行的insert或查询的流水结果表
	 * @return
	 */
	public String topTbName(String tbname){
		String tbName = null;
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
//			使用倒叙排序的方法，查找最新执行的流水表表名
			ps = ct.prepareStatement("select table_name from information_schema.tables where TABLE_NAME like '%testWithDefaultPath_IoTDB_lm_"+tbname+"_%'  order by (create_time) desc limit 1;");
			rs = ps.executeQuery();
			if(rs.next()){
				insertResultBean irb = new insertResultBean();
				tbName = rs.getString("table_name");
				
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		return tbName;
	}
	
	/**
	 * 查询每个操作的最近3个表名，用来获得最近3次的rate 和 lantency值
	 * @param tbname
	 * @return
	 */
	public String[] nearby3lr (String tbname){ //根据操作类型输入该类型的表名关键字，获得最近3次该类型操作的表的具体名称
		String[] nearby3tb = {"","",""};
		int i = 0;
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
//			使用倒叙排序的方法，查找最新执行的流水表表名
			ps = ct.prepareStatement("select table_name from information_schema.tables where TABLE_NAME like '%testWithDefaultPath_IoTDB_lm_"+tbname+"_%'  order by (create_time) desc limit 3;");
			rs = ps.executeQuery();
			while(rs.next()){
				insertResultBean irb = new insertResultBean();
				nearby3tb[i] = rs.getString("table_name");
				i = i+1;
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		
		return nearby3tb;
	}
	
	
	/**
	 * 查询执行insert或查询的mysql结果记录
	 * @param tbname
	 * @return
	 */
	public ArrayList fetchResultTB(String tbname){
		ArrayList alresult = new ArrayList();
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
			ps = ct.prepareStatement("SELECT * FROM "+tbname+"");
			rs = ps.executeQuery();
			while(rs.next()){
				insertBean ib = new insertBean();
				ib.setId(rs.getInt("id"));
				ib.setRecordTime(rs.getString("recordTime"));
				ib.setClientName(rs.getString("clientName"));
				ib.setOperation(rs.getString("operation"));
				ib.setOkPoint(rs.getInt("okPoint"));
				ib.setFailPoint(rs.getInt("failPoint"));
				ib.setLatency(rs.getDouble("latency"));
				ib.setRate(rs.getDouble("rate"));
				ib.setRemark(rs.getString("remark"));
				alresult.add(ib);
				
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		return alresult;
	}

	/**
	 * 从final_result表中读取本次测试的分析数据
	 * @param tbname
	 * @return
	 */
	public ArrayList fetchAnalyzTB(String tbname,String operator){
		ArrayList alresult = new ArrayList();
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
			ps = ct.prepareStatement("SELECT * FROM FINAL_RESULT where projectID = '"+tbname+"' and operation = '"+operator+"' ");
			rs = ps.executeQuery();

			while(rs.next()){
				analyzeResultBean arb = new analyzeResultBean();
				arb.setId(rs.getInt("id"));
				arb.setProjectID(rs.getString("projectID"));
				arb.setOperation(rs.getString("operation"));
				arb.setResult_key(rs.getString("result_key"));
				arb.setResult_value(rs.getString("result_value"));
				alresult.add(arb);
				
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		return alresult;
	}
	
	/**
	 * 从分析表的本次查询结果过滤
	 * @param tbname
	 * @param operator
	 * @param analyzeResult
	 * @return
	 */
	public ArrayList filterOperator(String tbname,String operator,ArrayList analyzeResult){
		ArrayList alruselt = new ArrayList();
		for(int i=0;i<analyzeResult.size();i++){
			analyzeResultBean arb = (analyzeResultBean)analyzeResult.get(i);
			analyzeResultBean ab = new analyzeResultBean();
			ab.setResult_key(arb.getResult_key());
			ab.setResult_value(arb.getResult_value());
			alruselt.add(ab);
		}
		return alruselt;
	}
	
	/**
	 * 从result_key和operation得到resutl_value
	 * @param result_key
	 * @param operation
	 * @return
	 */
	public String fetchAnalyzeValue(String result_key,String operation){
		String value = "";
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
			ps = ct.prepareStatement("select result_value from FINAL_RESULT where operation = '"+operation+"' and result_key = '"+result_key+"'");
			rs = ps.executeQuery();
			if(rs.next()){
				value = rs.getString("result_value");

			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		
		return value;
	}
	
	/**
	 * 从指定流水表获得 响应时间 和 查询速率 平均值（精确到3位小数）
	 * @param tbname
	 * @return
	 */
	public String[] fetchMsPs(String tbname){
		double latency =0.0 ;
		double rate =0.0;
		int count = 0;
		double latencyp =0.0;
		double ratep =0.0;
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
			ps = ct.prepareStatement("SELECT latency,rate FROM "+tbname+";");
			rs = ps.executeQuery();
			while(rs.next()){
				latency = latency + rs.getDouble("latency");
				rate = rate + rs.getDouble("rate");
				count = count + 1;
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		latencyp = latency / count;
		ratep = rate / count;
		DecimalFormat df = new DecimalFormat("#.000");
		String[] msps = {df.format(latencyp),df.format(ratep)};
		return msps;
	}
	
	/**
	 * 从表名获得该次测试的执行时间
	 * @param tbname
	 * @return
	 */
	public String testDate(String tbname){
		String date = "";
		String[] str=tbname.split("_");
		date = str[4]+"_"+str[5]+"_"+str[6];
		return date;
	}

	//获得当前操作的实际吞吐量
	public String fetchThroughput(String tbname,String operation){
		String result = "";
		try {
			Class.forName(dbdriver);
			ct = DriverManager.getConnection(dblink, user, password);
			ps = ct.prepareStatement("SELECT RESULT_VALUE FROM FINAL_RESULT WHERE RESULT_KEY = 'throughput' AND projectID = '"+tbname+"' AND operation = '"+operation+"'");
			rs = ps.executeQuery();
			if(rs.next()){
				result=rs.getString("RESULT_VALUE");
				
			}
		} catch (Exception efetchResultInsertTB) {
			efetchResultInsertTB.printStackTrace();
		}
		finally{
			try {
				if(rs!=null) rs.close();
				if(ps!=null) ps.close();
				if(ct!=null) ct.close();
			} catch (Exception eCloseDb) {
				eCloseDb.printStackTrace();
			}
		}
		return result;
	}
}
