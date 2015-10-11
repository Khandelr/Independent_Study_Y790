package com.example1;

import java.io.IOException;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GenerateImage {
	public static void main(String args[]) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		String itr = "47_20_1_2";
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = admin.getTableDescriptor(itr.getBytes());
		HColumnDescriptor hcd = htd.getFamily("pixels".getBytes());
		byte[] tablename = htd.getName();
		HTable table = new HTable(config, tablename);

		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
//		float image1[][] = new float[10100][10100];
		float image2[][] = new float[10100][10100];
//		float image3[][] = new float[10100][10100];
		int j = 0, k = 0;
//		Path pt=new Path("hdfs://10.23.3.241/user/root/output1/"+itr+".txt");
//		Configuration conf=new Configuration();
//		org.apache.hadoop.fs.FileSystem fs =pt.getFileSystem(conf);
//        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                                   // TO append data to a file, use fs.append(Path f)
     
//        br.write(itr);
//		br.close();
		for (Result r = rs.next(); r != null; r = rs.next()) {
			//k = 0;
			//System.out.println(Bytes.toString(r.getRow()));
			int ro=Integer.parseInt(Bytes.toString(r.getRow()).substring(3));
			//System.out.println(ro);
			for (KeyValue keyValue : r.list()) {
				//System.out.println(Bytes.toString(keyValue.getQualifier()));
				int co=Integer.parseInt(Bytes.toString(keyValue.getQualifier()).substring(5));
				//System.out.println(co);
				String pixels = Bytes.toString(keyValue.getValue());
				//System.out.println(pixels);
				String pixel[] = pixels.split(",");
//				br.write(pixels);
//				image1[j][k] = Float.parseFloat(pixel[0]);
				image2[ro][co] = Float.parseFloat(pixel[1]);
//				image3[j][k] = Float.parseFloat(pixel[2]);

				// System.out.println(pixels+"????"+pixel[0]+":"+pixel[1]+":"+pixel[2]);
				//k++;
			}
//			j++;
		}
//			FloatProcessor newImage1 = new FloatProcessor(image1);
//			FileSaver jpgFile1=new FileSaver(new ImagePlus("new_image1",newImage1));
//			jpgFile1.saveAsJpeg("/home/ubuntu/Images/convertedJpgImage1.jpg");
			FloatProcessor newImage2 = new FloatProcessor(image2);
			FileSaver jpgFile2=new FileSaver(new ImagePlus("new_image2",newImage2));
			jpgFile2.saveAsJpeg("/home/ubuntu/Images/conJpgImage2-20.jpg");
//			FloatProcessor newImage3 = new FloatProcessor(image1);
//			FileSaver jpgFile3=new FileSaver(new ImagePlus("new_image3",newImage3));
//			jpgFile3.saveAsJpeg("/home/ubuntu/Images/convertedJpgImage3.jpg");
	}
}
