package com.example1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
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
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;*/
import org.gdal.gdalconst.gdalconstConstants;
//import org.gdal.ogr.ogr;
import org.gdal.gdal.*;
import org.gdal.ogr.*;

public class WordCount {
	static Path folderPath;
	static Configuration conf = new Configuration();
	static org.apache.hadoop.fs.FileSystem f;
	static Path local = new Path("/home/ubuntu/input1");
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

		private final static IntWritable one = new IntWritable(1); // type of
																	// output
																	// value
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); // line
																			// to
																			// string
																			// token
			// System.out.println(itr);
			// context.write(itr,"1");
			// while (itr.hasMoreTokens()) {
			// word.set(itr.nextToken()); // set word as each input keyword
			// context.write(word, one); // create a pair
			// }
			f.copyToLocalFile(folderPath, new Path("/home/ubuntu/input1"));
			//f=new LocalFileSystem();
			/*System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libgdalconstjni");
			System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libgdaljni");
			System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libogrjni");
			System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libosrjni");*/
			//System.loadLibrary("/home/ubuntu/Programs/gdal-1.9.2/swig/java/gdal.jar");
			//DistributedCache.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/gdal.jar"), conf);
			org.gdal.gdal.gdal.AllRegister();
			ogr.RegisterAll();
			Dataset data;
			Band band;
			
			int[] intArray = null;
			/*float[] floatArray = null;
			int[] intArray1 = null;*/
			data = gdal.Open(local.toString()+"/SETSM_ArcticDEM_48_17_1_5_Date.tif",
					gdalconstConstants.GA_ReadOnly);
			if (data != null)
				System.out.println("kat gaya");
			System.out.println(data.getRasterCount());
			band = data.GetRasterBand(1);
			int xsize = band.getXSize();
			int ysize = band.getYSize();
			intArray = new int[xsize * ysize];
			band.ReadRaster(0, 0, xsize, ysize, intArray);
			for (int i = 0; i < intArray.length; i++) {
				byte[] row1 = Bytes.toBytes("row"+i);
				Put p1 = new Put(row1);
				
				p1.add(databytes, Bytes.toBytes(i), Bytes.toBytes(intArray[i]));
				table.put(p1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		args = new String[2];
		args[0] = "hdfs://10.39.1.68/user/root/input1";
		args[1] = "hdfs://10.39.1.68/user/root/output";
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
//		String[] otherArgs = new GenericOptionsParser(conf, args)
//				.getRemainingArgs(); // get all args
//		if (otherArgs.length != 2) {
//			System.err.println("Usage: WordCount  ");
//			System.exit(2);
//		}
		folderPath = new Path(args[0]);
		f = folderPath.getFileSystem(conf);

		Job job = new Job(conf, "wordcount");
		/*job.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/libgdalconstjni.so "));
		job.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/libgdaljni.so"));
		job.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/libogrjni.so"));
		job.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/libosrjni.so"));
		job.addFileToClassPath(new Path("hdfs://10.39.1.68/user/root/gdal.jar"));*/
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//job.addFileToClassPath(new Path("/user/root/gdal.jar"));
		
		FileInputFormat.addInputPath(job, folderPath);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(local.toString()+"/SETSM_ArcticDEM_48_17_1_5_Date.tif");
		boolean b = job.waitForCompletion(true);

		System.out.println(b);
		System.exit(b ? 0 : 1);
	}
}
