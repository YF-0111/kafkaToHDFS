package com.rogerguo.kafka.test.consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hdfs.*;
import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.Scanner;
import java.util.Timer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Input_hdfs {
    public static FileSystem fs;
    static Configuration conf = new Configuration();

    public static DistributedFileSystem dfs=new DistributedFileSystem();
    // public static com.rogerguo.test.HBaseDriver hBaseDriver = new com.rogerguo.test.HBaseDriver("118.25.11.198");
    static Configuration cfg = HBaseConfiguration.create();
    // static {
    //     cfg.set("hbase.zookeeper.quorum", "118.25.11.198");
    //     cfg.set("hbase.zookeeper.property.clientPort", "2181");
    //     conf.setBoolean("dfs.support.append", true);
    //     conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    // }
    public static void init_hdfs() throws Exception{
        //创建一个客户端对象
        String HDFS_PATH = "node1:9000";
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("fs.defaultFS", HDFS_PATH);//conf.set("参数名", 参数值);
        fs = FileSystem.get(conf);
        //System.out.println("init");
    }

    public static boolean isPathExist(String path) throws IOException {
        if (fs.exists(new Path(path))) {
            return true;
        } else {
            return false;
        }
    }

    public static void CreateFile(String hdfs_path) throws Exception{
        //System.out.println("hdfs_path:"+hdfs_path);
        Path path = new Path(hdfs_path);
        fs.create(path);
        boolean res = fs.exists(path);
        //System.out.println("目录创建结果："+(res?"创建成功":"创建失败"));
    }

    public static void read_file() throws Exception {
        try {
            String path = "./data/1";        //要遍历的路径
            File file = new File(path);        //获取其file对象
            File[] fs_list = file.listFiles();    //遍历path下的文件和目录，放在File数组中

            // 遍历文件
            for (File f : fs_list) {                    //遍历File[]数组
                if (!f.isDirectory()) {        //若非目录(即文件)，则打印
                    //System.out.println("f="+f.getName());
                    String name = f.getName();
                    String txt = "txt";
                    String[] file_id ;String delimeter_id = "\\.";  // 指定分割字符
                    file_id = name.split(delimeter_id); // 分割字符串
//                    //System.out.println("id="+file_id[0]);
                    String hdfs_path = "/test/" + file_id[0]+".txt";
                    //System.out.println("hdfs_path:"+hdfs_path+"\n"+"exist?"+isPathExist(hdfs_path));
                    Path hdfswritepath = new Path(hdfs_path);

                    if(txt.equals(file_id[1])){
                        FSDataOutputStream outputStream;
                        outputStream=fs.create(hdfswritepath);

                        InputStreamReader reader = new InputStreamReader(
                                new FileInputStream(f)); // 建立一个输入流对象reader
                        BufferedReader br = new BufferedReader(reader); // 建立一个对象，它把文件内容转成计算机能读懂的语言

                        String line = "";
                        line = br.readLine();

                        while (line != null) {
//                            //System.out.println("line = "+line);
                            String[] temp;
                            String delimeter = ",";  // 指定分割字符
                            temp = line.split(delimeter); // 分割字符串

                            String taxi_id, date_time, longitude, latitude;
                            taxi_id = temp[0];
                            date_time = temp[1];
                            longitude = temp[2];
                            latitude = temp[3];
                            String fileContent = taxi_id + date_time + longitude +latitude;

                            //==== Write file

                            //Cassical output stream usage
                            outputStream.writeBytes(fileContent);
//                            outputStream.close();

                            line = br.readLine();
                        }
                        outputStream.close();
                    }
                }
            }
        }
        catch(Exception e){
            //System.out.println(e);
        }
    }

    public static long get_file_length(String file_path) throws IOException {
        FileStatus[] in = fs.listStatus(new Path(file_path));
        long len;
        len = in[0].getLen();
        //System.out.println("len=" + len );
//        fs.close();
        return len;
    }

    public static void build_index() throws Exception {
        String index_file_path = "/test/index/index.txt";
        if(!isPathExist(index_file_path)){
            CreateFile(index_file_path);
        }
        // 先选定一个文件，然后，有block的话就取出首尾信息，存进hbase

    }

    public static void write_trajectory(String trajectory, String vehicle_file_name) throws IOException {
        Path vehicle_file_path = new Path(vehicle_file_name);
        File file = new File(vehicle_file_name);
        FSDataOutputStream outputStream;
        outputStream = fs.create(vehicle_file_path);
        //System.out.println("Flie:"+file);
        //System.out.println("t:"+trajectory);
        outputStream.writeBytes(trajectory);
        outputStream.close();
    }

    public static int get_block_num(String input) throws Exception {
        int block_num = 0;
        try{
            FileSystem fs=
                    FileSystem.get(
                            URI.create(input),conf);
            HdfsDataInputStream hdis = (HdfsDataInputStream)
                    fs.open(new Path(input));
            List<LocatedBlock> allBlocks=
                    hdis.getAllBlocks();

            for(LocatedBlock block:allBlocks){
                ExtendedBlock eBlock= block.getBlock();
                block_num ++;
            }
        }catch(IOException e){
            block_num = 0;
        }
        return block_num;
    }
}
