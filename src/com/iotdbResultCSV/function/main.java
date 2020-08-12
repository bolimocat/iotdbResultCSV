package com.iotdbResultCSV.function;

import java.util.ArrayList;

import com.iotdbResultCSV.dom.analyzeResultBean;
import com.iotdbResultCSV.dom.insertBean;
import com.iotdbResultCSV.dom.latencyrateRecordBean;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("读取mysql中的结果内容，转换为csv文件。");
//		java -jar iotdbResultCSV.jar "192.168.130.19" "weekly_test" "root" "Ise_Nel_2017" "Q5" "d:\\" "Q5"
		//参数加载 暂时固定访问mysql数据库,故driver为写死的mysql
		String dbip = args[0]; //数据库访问ip
		String dbname = args[1];//访问的数据库名，现在使用130.19上的weekly_test库
		String user = args[2];//数据库用户 root
		String password = args[3];//数据库密码
		String recordType = args[4]; //insert Q1 - 9，在jenkins上执行的操作，从insert场景到后续查询场景，查询场景对应mysql中
//		final_result的operation，不能一一对应会报空指针错误。
		String recordPath = args[5];//存放csv文件的目录
		String operator = ""; //初始化operation字段的值，使用时根据recordType变化
		
		//初始化类
//		connectMysql cm = new connectMysql("192.168.130.19","weekly_test","root","Ise_Nel_2017");
		connectMysql cm = new connectMysql(dbip,dbname,user,password); 
//		recordCSV rc = new recordCSV("192.168.130.19","weekly_test","root","Ise_Nel_2017");
		recordCSV rc = new recordCSV(dbip,dbname,user,password);
		
		ArrayList alInsert = new ArrayList();
		ArrayList alAnalyze = new ArrayList();
		ArrayList alAnalyzeResult = new ArrayList();
		ArrayList alLatencyRateRecord = new ArrayList();
		
//		String recordType = "Q8";
//		String operator = "";
		
		//读取本次执行测试所产生的流水表
		alInsert = cm.fetchResultTB(cm.topTbName(recordType));
//		alInsert = cm.fetchResultTB(cm.topTbName("Q8"));
		
//		根据recordType的内容，选择operator的内容，operator和final_result中operation的字段内容对应
		if(recordType.equals("insert")){
			operator="INGESTION";
		}
		if(recordType.equals("Lins")){
			operator="INGESTION";
		}
		if(recordType.equals("Q1")){
			operator="PRECISE_QUERY";		
				}
		if(recordType.equals("Q2")){
			operator="RANGE_QUERY";
		}
		if(recordType.equals("Q3")){
			operator="VALUE_RANGE_QUERY";
		}
		if(recordType.equals("Q4")){
			operator="AGG_RANGE_QUERY";
		}
		if(recordType.equals("Q5")){
			operator="AGG_VALUE_QUERY";
		}
		if(recordType.equals("Q6")){
			operator="AGG_RANGE_VALUE_QUERY";
		}
		if(recordType.equals("Q7")){
			operator="GROUP_BY_QUERY";
		}
		if(recordType.equals("Q8")){
			operator="LATEST_POINT_QUERY";
		}
		
		//根据最新执行的测试所产生的流水表，得到final_result表中分析信息的结果集
		alAnalyze = cm.fetchAnalyzTB(cm.topTbName(recordType), operator);
		//从最新的执行结果集中，根据选择的操作（insert ，Q1 - Q2），查找到的一批有记录的结果集
		
		alAnalyzeResult = cm.filterOperator(cm.topTbName(recordType),operator,alAnalyze);
		
		

		
//		将本次测试执行的流水信息写入csv文件，一个操作记录一个文件，使用操作类型和时间做文件名区分
		rc.recordCSV(recordPath, recordType, alInsert);
//		写入analyze的
//		初始化文件
//		rc.initAnalyzeCSV(recordPath,recordType);
//		将本次所有测试的分析结果汇总到一个相同的csv文件
//		分析文件只写数据，为避免重复，在环境中使用sh文件，初始化csv的表头
//		String throughput = cm.fetchThroughput(cm.topTbName(recordType), operator);
		rc.recordAnalyzeCSV(recordPath,recordType,alAnalyzeResult,operator);
		
		
//		for(int i=0;i<alAnalyze.size();i++){
//			analyzeResultBean arb = (analyzeResultBean)alAnalyze.get(i);
//			System.out.println(""+arb.getId()+" "+arb.getProjectID()+" "+arb.getOperation()+" "+arb.getResult_key()+" "+arb.getResult_value());
//		}
		
		
//		cm.recordCSV(recordPath, recordType,alInsert);
//		for(int i=0;i<cm.fetchAnalyzTB(cm.topTbName("Q8"),"Q8").size();i++){
//			analyzeResultBean arb = (analyzeResultBean)cm.fetchAnalyzTB(cm.topTbName("Q8"),"Q8").get(i);
//			System.out.println(""+arb.getId()+"_"+arb.getProjectID()+"_"+arb.getOperation()+"_"+arb.getResult_key());
//		}
		
//		cm.recordAnalyzeCSV("d:\\","Q8",alAnalyze);
		
		
//		String[] mp =cm.fetchMsPs(cm.topTbName(recordType));
		//记录latency和rate
//		rc.recordLatencyRateCSV(recordPath,mp, operator);
//		System.out.println("数组长度："+mp.length);
//		for(int i=0;i<mp.length;i++){
//			System.out.println(i+" -- "+mp[i]);
//		}
		
		//获取最新操作的3个表的表名
		String[] newest3tb=cm.nearby3lr(recordType);
		String testdate = "";
		for(int i=0;i<newest3tb.length;i++){
			System.out.println(" -- "+newest3tb[i]);
			testdate = cm.testDate(newest3tb[i]);//根据表名获取该操作的执行日期
			String[] mp =cm.fetchMsPs(newest3tb[i]);//获得该表中的lantency和rate平均值，取3位小数。
			rc.recordLatencyRateCSV(recordPath,mp, operator,testdate);//记录内容
		}
		
		String[] recordlist = {"insert","Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8"};
		//单独用来最后记录所有的latency和rate
		if(recordType.equals("Q8")){//判断当前执行到最后一个操作，开始写入latency和rate信息
			try {
				latencyrateRecordBean lrrb = new latencyrateRecordBean();
				for(int i=0;i<27;i++){
					if(i==0||i==1||i==2){
						lrrb.setOperator(recordlist[0]);//根据行号，得到操作名称
						String[] n3tb = cm.nearby3lr(recordlist[0]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
						
					}
					if(i==3||i==4||i==5){
						lrrb.setOperator(recordlist[1]);
						String[] n3tb = cm.nearby3lr(recordlist[1]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
						
					}
					if(i==6||i==7||i==8){
						lrrb.setOperator(recordlist[2]);
						String[] n3tb = cm.nearby3lr(recordlist[2]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==9||i==10||i==11){
						lrrb.setOperator(recordlist[3]);
						String[] n3tb = cm.nearby3lr(recordlist[3]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==12||i==13||i==14){
						lrrb.setOperator(recordlist[4]);
						String[] n3tb = cm.nearby3lr(recordlist[4]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==15||i==16||i==17){
						lrrb.setOperator(recordlist[5]);
						String[] n3tb = cm.nearby3lr(recordlist[5]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==18||i==19||i==20){
						lrrb.setOperator(recordlist[6]);
						String[] n3tb = cm.nearby3lr(recordlist[6]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==21||i==22||i==23){
						lrrb.setOperator(recordlist[7]);
						String[] n3tb = cm.nearby3lr(recordlist[7]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==24||i==25||i==26){
						lrrb.setOperator(recordlist[8]);
						String[] n3tb = cm.nearby3lr(recordlist[8]);//根据操作名称，得到最新的3个表
						for(int j=0;j<3;j++){//将这个操作的3个表的时间写入list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					alLatencyRateRecord.add(lrrb);
						
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		rc.recordLatencyRateCSVbyList("/home/liurui/lm_test", alLatencyRateRecord);
		
	}

//		java -jar iotdbResultCSV.jar "192.168.130.19" "weekly_test" "root" "Ise_Nel_2017" "Q5" "d:\\" "Q5"
}

