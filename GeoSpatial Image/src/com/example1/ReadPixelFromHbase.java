package com.example1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadPixelFromHbase {
	/*
		This function is used to read the pixels values from hbase and generate the image using ImageJ library.
	*/

	public static void main(String args[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		Configuration  config=HBaseConfiguration.create();
		HBaseAdmin admin=new HBaseAdmin(config);
		HTableDescriptor htd= admin.getTableDescriptor("47_20_1_4".getBytes());
		HColumnDescriptor hcd=htd.getFamily("pixels".getBytes());
		byte[] tablename = htd.getName();
		HTable table =  new HTable(config, tablename);
		for(int i=1;i<3;i++){
			byte [] row=Bytes.toBytes("row"+i);
			Get g=new Get(row);
			Result result=table.get(g);
			System.out.println("hi :"+result.cellScanner());

		}
		Scan scan=new Scan();
		ResultScanner rs=table.getScanner(scan);
		float image1[][]=new float[10100][10100];
		float image2[][]=new float[10100][10100];
		float image3[][]=new float[10100][10100];
		int j=0,k=0;
		for(Result r=rs.next();r!=null;r=rs.next()){
			k=0;
			for(KeyValue keyValue : r.list()) {
				String pixels=Bytes.toString(keyValue.getValue());
				String pixel[]=pixels.split(",");
				image1[j][k]=Float.parseFloat(pixel[0]);
				image2[j][k]=Float.parseFloat(pixel[1]);
				image3[j][k]=Float.parseFloat(pixel[2]);
				
		        k++;
		    }
			j++;
		}
		for (int i=0;i<image1.length;i++){
			for(int l=0;l<image1[i].length;l++){
				System.out.print(image1[i][l]+"\t");
			}
			
		}
		for (int i=0;i<image2.length;i++){
			for(int l=0;l<image2[i].length;l++){
				System.out.print(image2[i][l]+"\t");
			}
			
		}
		for (int i=0;i<image3.length;i++){
			for(int l=0;l<image3[i].length;l++){
				System.out.print(image3[i][l]+"\t");
			}
			
		}
	}
}
