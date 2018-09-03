package com.kevin.mapreduce.mr.friendRecom;

import com.kevin.mapreduce.constants.PathConst;
import com.kevin.mapreduce.utils.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * describe  : 简单好友推荐
 *
 * 输入文件：file/in_friend.txt
 * 输出文件：file/out_friend.txt
 *
 * creat_user: kevin
 * creat_time: 2018/8/29 23:22
 * email     : kevin_love_it@163.com
 **/
public class FofRecom {

    public static void main(String[] args) {
        try {
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);

            //设置job
            Job job = Job.getInstance(config, "friend recommendation");
            //job.setJarByClass(FofRecom.class);
            job.setJar(PathConst.JAR_OUTPUT_PATH);

            //设置mapper相关类
            job.setMapperClass(FofMapper.class);
            job.setMapOutputKeyClass(Fof.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reducer相关属性  
            job.setReducerClass(FofReducer.class);
            job.setOutputKeyClass(Fof.class);
            job.setOutputValueClass(IntWritable.class);

            //设置key,value分隔符
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //设置输入输出目录
            FileInputFormat.addInputPath(job, new Path("/input/in_friend.txt"));
            Path outPath = new Path("/output/out_friend");
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            //提交任务
            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job执行成功");
            }
        } catch (Exception e) {
            LoggerUtil.error("job执行失败");
            e.printStackTrace();
        }
    }

    static class FofMapper extends Mapper<Text, Text, Fof, IntWritable> {

        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            //用户
            String user = key.toString();
            //用户所有的好友列表
            String[] friends = StringUtils.split(value.toString(), "\t");
            //好友之间的FOF关系矩阵
            for (int i = 0; i < friends.length; i++) {
                //好友1
                String f1 = friends[i];
                //用户的好友列表
                Fof alreadyFriends = new Fof(user, f1);
                //输出好友列表，值为0。方便在reduce阶段去除已经是好友的FOF关系。
                context.write(alreadyFriends, new IntWritable(0));
                for (int j = i + 1; j < friends.length; j++) {
                    //好友2
                    String f2 = friends[j];
                    Fof fof = new Fof(f1, f2);
                    //输出好友之间的FOF关系列表，值为1，方便reduce阶段累加
                    context.write(fof, new IntWritable(1));
                }
            }
        }
    }

    static class FofReducer extends Reducer<Fof, IntWritable, Fof, IntWritable> {

        protected void reduce(Fof fof, Iterable<IntWritable> itr, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            boolean f = true;
            for (IntWritable i : itr) {
                //已经是好友关系
                if (i.get() == 0) {
                    f = false;
                    break;
                } else {
                    //累计，统计FOF的系数
                    sum = sum + i.get();
                }
            }
            //已经是好友关系的，不再重复推介
            if (f) {
                //输出key为潜在好友对，值为出现的次数
                context.write(fof, new IntWritable(sum));
            }
        }
    }
}
