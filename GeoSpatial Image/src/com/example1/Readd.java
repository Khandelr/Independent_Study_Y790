package com.example1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example1.Test.Map;

public class Readd {
	static Path folderPath,outputPath;
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
	/*
		This is the mapper function which is used to read the pixels values from
		the text files in hdfs and store the same in hbase.
	*/
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1); 
																	
		private Text word = new Text(); 

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String data[]=value.toString().split(";");
			System.out.println(value.toString());
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			System.out.println(fileName.substring(0, fileName.length()-4));
			String tableName=fileName.substring(0, fileName.length()-4);
			
			if(!admin.isTableAvailable(tableName.getBytes())){
				HTableDescriptor htd = new HTableDescriptor(tableName);
				HColumnDescriptor hcd = new HColumnDescriptor("pixels");
				htd.addFamily(hcd);
				admin.createTable(htd);
				byte[] tablename = htd.getName();
				HTable table = new HTable(config, tablename);
				
				byte[] row = Bytes.toBytes("row"+data[0]);
				
				for(int j=1;j<data.length;j++){
					Put p = new Put(row);
					byte[] databytes = Bytes.toBytes("pixels");
					p.add(databytes, Bytes.toBytes("pixel"+j), Bytes.toBytes(data[j]));
					table.put(p);
				}
				}
				else{
					System.out.println("1");
					HTableDescriptor htd=admin.getTableDescriptor(tableName.getBytes());
					HColumnDescriptor hcd=htd.getFamily("pixels".getBytes());
					byte[] tablename = htd.getName();
					HTable table =  new HTable(config, tablename);
					byte[] row = Bytes.toBytes("row"+data[0]);
					
						for(int j=1;j<data.length;j++){
							Put p = new Put(row);
							byte[] databytes = Bytes.toBytes("pixels");
							p.add(databytes, Bytes.toBytes("pixel"+j), Bytes.toBytes(data[j]));
							table.put(p);
						}
					
				}
			
	}
	}
	/*
		This function is used to submit a map reduce job to read 
		the pixel values and store the same in hbase.
		The input path of hdfs is set in args[0]
		and output path of hdfs is set in args[1]
	*/
	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "hdfs://10.23.3.241/user/root/input";
		args[1] = "hdfs://10.23.3.241/user/root/output";

		folderPath = new Path(args[0]);
		outputPath=new Path(args[1]);
		f = folderPath.getFileSystem(conf);

		Job job = new Job(conf, "wordcount");
		
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, folderPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean b = job.waitForCompletion(true);

		System.out.println(b);
		System.exit(b ? 0 : 1);
	}
}
