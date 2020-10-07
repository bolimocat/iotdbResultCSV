package com.iotdbResultCSV.function;

import java.util.ArrayList;

import com.iotdbResultCSV.dom.analyzeResultBean;
import com.iotdbResultCSV.dom.configBean;
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
		if(recordType.equals("insert")||recordType.equals("isover")||recordType.equals("noover")){
			operator="INGESTION";
		}
		if(recordType.equals("q1")){
			operator="PRECISE_QUERY";		
				}
		if(recordType.equals("q21")||recordType.equals("q22")||recordType.equals("q23")){
			operator="RANGE_QUERY";
		}
		if(recordType.equals("q31")||recordType.equals("q32")||recordType.equals("q33")){
			operator="VALUE_RANGE_QUERY";
		}
		if(recordType.equals("q4a1")||recordType.equals("q4a2")||recordType.equals("q4a3")||recordType.equals("q4b1")||recordType.equals("q4b2")||recordType.equals("q4b3")){
			operator="AGG_RANGE_QUERY";
		}
		if(recordType.equals("q5")){
			operator="AGG_VALUE_QUERY";
		}
		if(recordType.equals("q61")||recordType.equals("q62")||recordType.equals("q63")){
			operator="AGG_RANGE_VALUE_QUERY";
		}
		if(recordType.equals("q71")||recordType.equals("q72")||recordType.equals("q73")||recordType.equals("q74")){
			operator="GROUP_BY_QUERY";
		}
		if(recordType.equals("q8")){
			operator="LATEST_POINT_QUERY";
		}
		
		//根据最新执行的测试所产生的流水表，得到final_result表中分析信息的结果集
		alAnalyze = cm.fetchAnalyzTB(cm.topTbName(recordType), operator);
		//从最新的执行结果集中，根据选择的操作（insert ，Q1 - Q2），查找到的一批有记录的结果集
		
		alAnalyzeResult = cm.filterOperator(cm.topTbName(recordType),operator,alAnalyze);

		

		
//		将本次测试执行的流水信息写入csv文件，一个操作记录一个文件，使用操作类型和时间做文件名区分
//		rc.recordCSV(recordPath, recordType, alInsert);暂时不记录流水信息
//		写入analyze的
//		初始化文件
//		rc.initAnalyzeCSV(recordPath,recordType);
//		将本次所有测试的分析结果汇总到一个相同的csv文件
//		分析文件只写数据，为避免重复，在环境中使用sh文件，初始化csv的表头
//		String throughput = cm.fetchThroughput(cm.topTbName(recordType), operator);
//		最新修改为记录正常操作和错误操作数，正确写点和错误写点数，以及基本的吞吐量和平均延迟-暂时屏蔽
//		rc.recordAnalyzeCSV(recordPath,recordType,alAnalyzeResult,operator);
//		为confluence结果页记录AVG和吞吐
		rc.recordTPAVGCSV(recordPath,recordType,alAnalyzeResult,operator);
//		在一个文件里记录写入的最新mysql表表名
		rc.recordMySQLDBname( recordPath,cm.topTbName(recordType));
	}

//		java -jar iotdbResultCSV.jar "192.168.130.4" "weekly_test" "root" "Ise_Nel_2017" "q5" "d:\\"
}

