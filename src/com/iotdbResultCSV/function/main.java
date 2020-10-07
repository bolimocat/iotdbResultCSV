package com.iotdbResultCSV.function;

import java.util.ArrayList;

import com.iotdbResultCSV.dom.analyzeResultBean;
import com.iotdbResultCSV.dom.configBean;
import com.iotdbResultCSV.dom.insertBean;
import com.iotdbResultCSV.dom.latencyrateRecordBean;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("��ȡmysql�еĽ�����ݣ�ת��Ϊcsv�ļ���");
//		java -jar iotdbResultCSV.jar "192.168.130.19" "weekly_test" "root" "Ise_Nel_2017" "Q5" "d:\\" "Q5"
		//�������� ��ʱ�̶�����mysql���ݿ�,��driverΪд����mysql
		String dbip = args[0]; //���ݿ����ip
		String dbname = args[1];//���ʵ����ݿ���������ʹ��130.19�ϵ�weekly_test��
		String user = args[2];//���ݿ��û� root
		String password = args[3];//���ݿ�����
		String recordType = args[4]; //insert Q1 - 9����jenkins��ִ�еĲ�������insert������������ѯ��������ѯ������Ӧmysql��
//		final_result��operation������һһ��Ӧ�ᱨ��ָ�����
		String recordPath = args[5];//���csv�ļ���Ŀ¼
		String operator = ""; //��ʼ��operation�ֶε�ֵ��ʹ��ʱ����recordType�仯
		
		//��ʼ����
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
		
		//��ȡ����ִ�в�������������ˮ��
		alInsert = cm.fetchResultTB(cm.topTbName(recordType));
//		alInsert = cm.fetchResultTB(cm.topTbName("Q8"));
		
//		����recordType�����ݣ�ѡ��operator�����ݣ�operator��final_result��operation���ֶ����ݶ�Ӧ
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
		
		//��������ִ�еĲ�������������ˮ���õ�final_result���з�����Ϣ�Ľ����
		alAnalyze = cm.fetchAnalyzTB(cm.topTbName(recordType), operator);
		//�����µ�ִ�н�����У�����ѡ��Ĳ�����insert ��Q1 - Q2�������ҵ���һ���м�¼�Ľ����
		
		alAnalyzeResult = cm.filterOperator(cm.topTbName(recordType),operator,alAnalyze);

		

		
//		�����β���ִ�е���ˮ��Ϣд��csv�ļ���һ��������¼һ���ļ���ʹ�ò������ͺ�ʱ�����ļ�������
//		rc.recordCSV(recordPath, recordType, alInsert);��ʱ����¼��ˮ��Ϣ
//		д��analyze��
//		��ʼ���ļ�
//		rc.initAnalyzeCSV(recordPath,recordType);
//		���������в��Եķ���������ܵ�һ����ͬ��csv�ļ�
//		�����ļ�ֻд���ݣ�Ϊ�����ظ����ڻ�����ʹ��sh�ļ�����ʼ��csv�ı�ͷ
//		String throughput = cm.fetchThroughput(cm.topTbName(recordType), operator);
//		�����޸�Ϊ��¼���������ʹ������������ȷд��ʹ���д�������Լ���������������ƽ���ӳ�-��ʱ����
//		rc.recordAnalyzeCSV(recordPath,recordType,alAnalyzeResult,operator);
//		Ϊconfluence���ҳ��¼AVG������
		rc.recordTPAVGCSV(recordPath,recordType,alAnalyzeResult,operator);
//		��һ���ļ����¼д�������mysql�����
		rc.recordMySQLDBname( recordPath,cm.topTbName(recordType));
	}

//		java -jar iotdbResultCSV.jar "192.168.130.4" "weekly_test" "root" "Ise_Nel_2017" "q5" "d:\\"
}

