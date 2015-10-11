package com.example1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.gdal.ogr.ogr;

public class ReadParallel implements Runnable {
	static ArrayList<String> folder = new ArrayList<String>();
	static String folderPath = "/home/ubuntu/Images/";
	private Thread t;
	private String threadName;
	static int x;
	static int y;

	ReadParallel(String name) {
		threadName = name;

	}

	static {
		folder.add("48_17_1_5");
		// folder.add("47_20_1_1");
		folder.add("47_20_1_2");
		folder.add("47_20_1_3");
		folder.add("47_20_1_4");
		folder.add("47_20_2_1");
		folder.add("47_20_2_2");
		folder.add("47_20_2_3");
		folder.add("47_20_2_4");
		System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libgdalconstjni.so");
		System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libgdaljni.so");
		System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libogrjni.so");
		System.load("/home/ubuntu/Programs/gdal-1.9.2/swig/java/libosrjni.so");

	}

	public static void main(String[] args) throws InterruptedException {
		List<ReadParallel> l = new ArrayList<ReadParallel>();
		List<Thread> tl = new ArrayList<Thread>();
		for (int i = 0; i < 4; i++) {
			l.add(new ReadParallel(i + ""));
		}
		for (int i = 0; i < 4; i++) {
			l.get(i).t = new Thread(l.get(i));
			tl.add(l.get(i).t);
			l.get(i).t.start();
			l.get(i).t.join();
		}
	}
	/*
	 * This function is used to read the images from the local path using Gdal library and store the 
	 * data in the text files.
	 * 
	 */
	public void run() {
		System.out.println(this.threadName);
		for (int k = 0; k < folder.size(); k++) {
			int n = Integer.parseInt(this.threadName);
			if (n % 4 == k) {
				gdal.AllRegister();
				ogr.RegisterAll();
				Dataset data;
				Band band;
				File dir = new File(folderPath + folder.get(k));
				System.out.println(dir.getAbsolutePath());
				try {
					// for (File file : dir.listFiles()) {
					//
					// if (file.isDirectory()) {
					int[] intArray = null;
					float[] floatArray = null;
					int[] intArray1 = null;
					File subDir = new File(folderPath + folder.get(k));

					for (File images : subDir.listFiles()) {

						if (images.getName().endsWith("Date.tif")) {
							data = gdal.Open(images.getCanonicalPath(),
									gdalconstConstants.GA_ReadOnly);
							if (data != null)
								System.out.println("kat gaya");
							System.out.println(data.getRasterCount());
							band = data.GetRasterBand(1);
							int xsize = band.getXSize();
							int ysize = band.getYSize();
							x = xsize;
							y = ysize;
							intArray = new int[xsize * ysize];
							band.ReadRaster(0, 0, xsize, ysize, intArray);
						}
						if (images.getName().endsWith("DEM.tif")) {
							data = gdal.Open(images.getCanonicalPath(),
									gdalconstConstants.GA_ReadOnly);
							band = data.GetRasterBand(1);
							int xsize = band.getXSize();
							int ysize = band.getYSize();
							floatArray = new float[xsize * ysize];
							band.ReadRaster(0, 0, xsize, ysize, floatArray);
						}
						if (images.getName().endsWith("Match.tif")) {
							data = gdal.Open(images.getCanonicalPath(),
									gdalconstConstants.GA_ReadOnly);
							band = data.GetRasterBand(1);
							int xsize = band.getXSize();
							int ysize = band.getYSize();
							intArray1 = new int[xsize * ysize];
							band.ReadRaster(0, 0, xsize, ysize, intArray1);
						}
					}
					if (intArray != null && intArray1 != null
							&& floatArray != null) {
						String fn[]=dir.getAbsolutePath().split("/");
						File fileWrite = new File("/home/ubuntu/Images/TextFiles/" + fn[fn.length-1]
								+ ".txt");
						if (!fileWrite.exists()) {
							fileWrite.createNewFile();
						}

						FileWriter fw = new FileWriter(
								fileWrite.getAbsoluteFile());
						BufferedWriter bw = new BufferedWriter(fw);
						int l=1;
						bw.write(l+++";");
						for (int i = 0; i < intArray.length
								&& i < floatArray.length
								&& i < intArray1.length; i++) {
							if (i % y != 0) {
								bw.write(intArray[i] + "," + floatArray[i]
										+ "," + intArray1[i] + ";");
							} else {
								if(i!=0){
									bw.write("\n");
									bw.write(l+++";");
								}
							}
						}
//						for (int i = 0; i < 4040
//								&& i < 4040
//								&& i < 4040; i++) {
//							if (i % y != 0) {
//								bw.write(intArray[i] + "," + floatArray[i]
//										+ "," + intArray1[i] + ";");
//							} else {
//								if(i!=0){
//									bw.write("\n");
//									bw.write(l+++";");
//								}
//							}
//						}
						bw.close();

						System.out.println("Done");
					}
					// }

					// }
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}