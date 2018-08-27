package com.kevin.mapreduce.mr;

import com.kevin.mapreduce.utils.LoggerUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
 
/**
 * describe  : 列出/input下的所有文件
 * creat_user: zhangkai
 * creat_time: 2018/8/25 20:57
 * email     : kevin_love_it@163.com
 **/
public class Excise {

    public static void main(String[] args)throws Exception{
        LoggerUtil.info("列出hdfs上 /input 目录下的所有文件");
        String uri = "hdfs://192.168.72.39:9000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        FileStatus[] statuses = fs.listStatus(new Path("/input"));
        for (FileStatus status:statuses){
            System.out.println(status);
        }
    }
}