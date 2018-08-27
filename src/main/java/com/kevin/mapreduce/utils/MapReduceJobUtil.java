package com.kevin.mapreduce.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * describe  : mapreduce job工具类
 * creat_user: zhangkai
 * creat_time: 2018/8/26 22:46
 * email     : kevin_love_it@163.com
 **/
public class MapReduceJobUtil {

    /**
     * 构造mapreduce任务
     * @param conf Configuration
     * @param jobClazz  如：SortStep.class
     * @param args 输入输出路径，输入路径可包含多个文件，如：/input/* /output/sort
     * @param inputFormat 输入类型
     * @param mapperClass mapper类
     * @param mapKeyClass mapper的输出key类型
     * @param mapValueClass mapper的输出value类型
     * @param outputFormat 输出类型
     * @param reducerClass reducer类
     * @param outkeyClass  reducer的输出key类型
     * @param outvalueClass reducer的输出value类型
     * @param combinerClass combiner类
     * @return job
     * @throws IOException IO异常
     */
    public static Job buildJob(Configuration conf,
                               String[] args,
                               Class<?> jobClazz,
                               Class<? extends InputFormat> inputFormat,
                               Class<? extends Mapper> mapperClass,
                               Class<?> mapKeyClass,
                               Class<?> mapValueClass,
                               Class<? extends OutputFormat> outputFormat,
                               Class<? extends Reducer> reducerClass,
                               Class<?> outkeyClass,
                               Class<?> outvalueClass,
                               Class<? extends Reducer> combinerClass) throws IOException {

        String jobName = jobClazz.getSimpleName();
        Job job = Job.getInstance(conf, jobName);
        //设置job运行的jar
        job.setJarByClass(jobClazz);

        //解析输入输出参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        //设置整个程序的输入
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        //就是设置如何将输入文件解析成一行一行内容的解析类
        job.setInputFormatClass(inputFormat);

        //设置整个程序的输出
        //该段代码是用来判断输出路径存在不存在，存在就删除，虽然方便操作，但请谨慎
        //如果当前输出目录存在，删除之，以避免.FileAlreadyExistsException
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(outputFormat);

        //设置mapper
        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(mapKeyClass);
        job.setMapOutputValueClass(mapValueClass);

        //设置reducer，如果有才设置，没有的话就不用设置
        if (null != reducerClass) {
            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(outkeyClass);
            job.setOutputValueClass(outvalueClass);
        }

        //设置combiner，如果有才设置，没有的话不用设置
        if(null != combinerClass){
            job.setCombinerClass(combinerClass);
        }
        return job;
    }
}