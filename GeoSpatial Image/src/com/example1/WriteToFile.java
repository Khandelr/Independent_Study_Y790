package com.example1;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteToFile {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Path pt=new Path("hdfs://10.23.3.241/user/root/output1/abc.txt");
		Configuration conf=new Configuration();
		org.apache.hadoop.fs.FileSystem fs =pt.getFileSystem(conf);
        BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
                                   // TO append data to a file, use fs.append(Path f)
        String itr="hello hi bye";
        br.write(itr);
		br.close();
	}

}
