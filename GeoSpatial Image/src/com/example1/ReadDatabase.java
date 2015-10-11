package com.example1;

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example1.ReadFromDatabase.Map;

public class ReadDatabase {
	static Path folderPath, outputPath;
	static Configuration conf = new Configuration();
	static org.apache.hadoop.fs.FileSystem f;
	static Configuration config = HBaseConfiguration.create();
	static HBaseAdmin admin;

	static {
		try {
			admin = new HBaseAdmin(config);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("Inside Map function....");
			String itr = value.toString();
			Configuration config = HBaseConfiguration.create();
			HBaseAdmin admin = new HBaseAdmin(config);
			HTableDescriptor htd = admin.getTableDescriptor(itr.getBytes());
			HColumnDescriptor hcd = htd.getFamily("pixels".getBytes());
			byte[] tablename = htd.getName();
			HTable table = new HTable(config, tablename);

			Scan scan = new Scan();
			ResultScanner rs = table.getScanner(scan);
			float image1[][] = new float[10100][10100];
			float image2[][] = new float[10100][10100];
			float image3[][] = new float[10100][10100];
//		

//			for (Result r = rs.next(); r != null; r = rs.next()) {
//		
//				int ro=Integer.parseInt(Bytes.toString(r.getRow()).substring(3));
//
//				for (KeyValue keyValue : r.list()) {
//					int co=Integer.parseInt(Bytes.toString(keyValue.getQualifier()).substring(5));
//					String pixels = Bytes.toString(keyValue.getValue());
//					String pixel[] = pixels.split(",");
//					image1[ro][co] = Float.parseFloat(pixel[0]);
//					System.out.println(image1[ro][co]);
//					/*image2[ro][co] = Float.parseFloat(pixel[1]);
//					image3[ro][co] = Float.parseFloat(pixel[2]);*/
//
//				}
//			}
//				FloatProcessor newImage1 = new FloatProcessor(image1);
//				FileSaver jpgFile1=new FileSaver(new ImagePlus("new_image1",newImage1));
//				jpgFile1.saveAsJpeg("/home/ubuntu/Images/Date.jpg");
				/*FloatProcessor newImage2 = new FloatProcessor(image2);
				FileSaver jpgFile2=new FileSaver(new ImagePlus("new_image2",newImage2));
				jpgFile2.saveAsJpeg("/home/ubuntu/Images/DEM.jpg");
				FloatProcessor newImage3 = new FloatProcessor(image1);
				FileSaver jpgFile3=new FileSaver(new ImagePlus("new_image3",newImage3));
				jpgFile3.saveAsJpeg("/home/ubuntu/Images/Match.jpg");*/
		}
	}

	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "hdfs://10.23.3.241/user/root/input2";
		args[1] = "hdfs://10.23.3.241/user/root/output";

		folderPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		f = folderPath.getFileSystem(conf);
		System.out.println("Starting...");
		Job job = new Job(conf, "wordcount");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
//		config.set("yarn.application.classpath", "/home/ubuntu/Programs/hadoop-2.6.0/etc/hadoop,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/common/lib/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/common/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/hdfs,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/hdfs/lib/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/hdfs/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/yarn/lib/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/yarn/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/mapreduce/lib/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/mapreduce/*,"
//				                               + "/contrib/capacity-scheduler/*.jar,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/yarn/*,"
//				                               + "/home/ubuntu/Programs/hadoop-2.6.0/share/hadoop/yarn/lib/*");
		FileInputFormat.addInputPath(job, folderPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean b = job.waitForCompletion(true);

		/*System.out.println(b);
		System.exit(b ? 0 : 1);*/
	}

}
