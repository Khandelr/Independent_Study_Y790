package com.example1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Test {

	static Path folderPath,outputPath;
	static Configuration conf = new Configuration();
	static org.apache.hadoop.fs.FileSystem f;
	//static Path local = new Path("/home/ubuntu/input2");
	static Configuration config = HBaseConfiguration.create();
	// Create table
	static HBaseAdmin admin;
	static HTableDescriptor htd = new HTableDescriptor("dikki21");
	static HColumnDescriptor hcd = new HColumnDescriptor("data1");
	// htd.addFamily(hcd);
	// admin.createTable(htd);
	static byte[] tablename;
	static HTableDescriptor[] tables;
	static HTable table;
	static byte[] databytes = Bytes.toBytes("data1");
	static {
		htd.addFamily(hcd);
//		conf.addResource(new Path("hdfs://10.39.1.68/user/root/gdal.jar"));
//		conf.addResource(new Path("hdfs://10.39.1.68/user/root/libgdalconstjni.so "));
//		conf.addResource(new Path("hdfs://10.39.1.68/user/root/libgdaljni.so"));
//		conf.addResource(new Path("hdfs://10.39.1.68/user/root/libogrjni.so"));
//		conf.addResource(new Path("hdfs://10.39.1.68/user/root/libosrjni.so"));
		
		try {
			admin = new HBaseAdmin(config);
			admin.createTable(htd);
			tablename= htd.getName();
			tables = admin.listTables();
			table = new HTable(config, tablename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		
	}
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1); 
																	
		private Text word = new Text(); 

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
            f.copyToLocalFile(new Path(folderPath+"/47_20_1_2__.txt"), new Path("/home/ubuntu/input2/."));
            //BufferedWriter br=new BufferedWriter(new OutputStreamWriter(f.create(new Path("/home/ubuntu/input3/t.txt"),true)));
            String line;
            BufferedReader b = null;
            b = new BufferedReader(new FileReader("/home/ubuntu/input2/47_20_1_2__.txt"));
			while ((line = b.readLine()) != null) {
				System.out.println(line);
				//br.write(line);
				String pixels[]=line.split(";");
				for(int i=0;i<pixels.length;i++){
					//String pixel[]=pixels[i].split(",");
					byte[] row1 = Bytes.toBytes("row"+i);
					Put p1 = new Put(row1);
					
					p1.add(databytes, Bytes.toBytes(i), Bytes.toBytes(pixels[i]));
					table.put(p1);
				}
				
			}
//            br.close();
			b.close();
		}
	}

	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "hdfs://10.39.1.68/user/root/input3";
		args[1] = "hdfs://10.39.1.68/user/root/output";

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
