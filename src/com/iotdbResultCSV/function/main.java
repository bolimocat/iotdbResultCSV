package com.iotdbResultCSV.function;

import java.util.ArrayList;

import com.iotdbResultCSV.dom.analyzeResultBean;
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
		
		//��������ִ�еĲ�������������ˮ���õ�final_result���з�����Ϣ�Ľ����
		alAnalyze = cm.fetchAnalyzTB(cm.topTbName(recordType), operator);
		//�����µ�ִ�н�����У�����ѡ��Ĳ�����insert ��Q1 - Q2�������ҵ���һ���м�¼�Ľ����
		
		alAnalyzeResult = cm.filterOperator(cm.topTbName(recordType),operator,alAnalyze);
		
		

		
//		�����β���ִ�е���ˮ��Ϣд��csv�ļ���һ��������¼һ���ļ���ʹ�ò������ͺ�ʱ�����ļ�������
		rc.recordCSV(recordPath, recordType, alInsert);
//		д��analyze��
//		��ʼ���ļ�
//		rc.initAnalyzeCSV(recordPath,recordType);
//		���������в��Եķ���������ܵ�һ����ͬ��csv�ļ�
//		�����ļ�ֻд���ݣ�Ϊ�����ظ����ڻ�����ʹ��sh�ļ�����ʼ��csv�ı�ͷ
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
		//��¼latency��rate
//		rc.recordLatencyRateCSV(recordPath,mp, operator);
//		System.out.println("���鳤�ȣ�"+mp.length);
//		for(int i=0;i<mp.length;i++){
//			System.out.println(i+" -- "+mp[i]);
//		}
		
		//��ȡ���²�����3����ı���
		String[] newest3tb=cm.nearby3lr(recordType);
		String testdate = "";
		for(int i=0;i<newest3tb.length;i++){
			System.out.println(" -- "+newest3tb[i]);
			testdate = cm.testDate(newest3tb[i]);//���ݱ�����ȡ�ò�����ִ������
			String[] mp =cm.fetchMsPs(newest3tb[i]);//��øñ��е�lantency��rateƽ��ֵ��ȡ3λС����
			rc.recordLatencyRateCSV(recordPath,mp, operator,testdate);//��¼����
		}
		
		String[] recordlist = {"insert","Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8"};
		//������������¼���е�latency��rate
		if(recordType.equals("Q8")){//�жϵ�ǰִ�е����һ����������ʼд��latency��rate��Ϣ
			try {
				latencyrateRecordBean lrrb = new latencyrateRecordBean();
				for(int i=0;i<27;i++){
					if(i==0||i==1||i==2){
						lrrb.setOperator(recordlist[0]);//�����кţ��õ���������
						String[] n3tb = cm.nearby3lr(recordlist[0]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
						
					}
					if(i==3||i==4||i==5){
						lrrb.setOperator(recordlist[1]);
						String[] n3tb = cm.nearby3lr(recordlist[1]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
						
					}
					if(i==6||i==7||i==8){
						lrrb.setOperator(recordlist[2]);
						String[] n3tb = cm.nearby3lr(recordlist[2]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==9||i==10||i==11){
						lrrb.setOperator(recordlist[3]);
						String[] n3tb = cm.nearby3lr(recordlist[3]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==12||i==13||i==14){
						lrrb.setOperator(recordlist[4]);
						String[] n3tb = cm.nearby3lr(recordlist[4]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==15||i==16||i==17){
						lrrb.setOperator(recordlist[5]);
						String[] n3tb = cm.nearby3lr(recordlist[5]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==18||i==19||i==20){
						lrrb.setOperator(recordlist[6]);
						String[] n3tb = cm.nearby3lr(recordlist[6]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==21||i==22||i==23){
						lrrb.setOperator(recordlist[7]);
						String[] n3tb = cm.nearby3lr(recordlist[7]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
							lrrb.setTime(cm.testDate(n3tb[j]));
							String[] mp = cm.fetchMsPs(n3tb[j]);
							lrrb.setLatency(mp[0]);
							lrrb.setRate(mp[1]);
						}
					}
					if(i==24||i==25||i==26){
						lrrb.setOperator(recordlist[8]);
						String[] n3tb = cm.nearby3lr(recordlist[8]);//���ݲ������ƣ��õ����µ�3����
						for(int j=0;j<3;j++){//�����������3�����ʱ��д��list
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

