package com.kevin.mapreduce.mr.friendRecom;

import com.kevin.mapreduce.constants.PathConst;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * describe  : 根据每一个用户与其他用户的共同好友个数从高到低排序
 *
 * 输入文件：file/in_friend.txt
 * 输出文件：file/out_friend2.txt
 *
 * creat_user: kevin
 * creat_time: 2018/9/3 22:20
 * email     : kevin_love_it@163.com
 **/
public class FOFMapperReducer {
    public static void main(String[] args) {
        try {
            Configuration config = new Configuration();
            FileSystem fs = FileSystem.get(config);
            //设置job
            Job job = Job.getInstance(config);
            //设置main方法所在的类
            //job.setJarByClass(FofRecom.class);
            job.setJar(PathConst.JAR_OUTPUT_PATH);
            job.setJobName("run2");

            //设置mapper相关类
            job.setMapperClass(SortMapper.class);
            job.setMapOutputKeyClass(User.class);
            job.setMapOutputValueClass(User.class);

            //设置reducer相关属性
            job.setReducerClass(SortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //设置排序分组
            job.setSortComparatorClass(FofSort.class);
            job.setGroupingComparatorClass(FoFGroup.class);

            //设置keyvalue分隔符
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //设置输入输出目录
            FileInputFormat.addInputPath(job, new Path("/input/in_friend2.txt"));
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
            e.printStackTrace();
        }
    }
    //map函数，每个用户的推介好友列表，并按推介指数从大到小排序
    static class SortMapper extends Mapper<Text,Text,User,User> {
        protected void map(Text key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String[] splits = StringUtils.split(value.toString(), '\t');
            String other = splits[0];       //推介的好友
            int friendsCount = Integer.parseInt(splits[1]);  // 该推介好友的推介系数
            context.write(new User(key.toString(), friendsCount), new User(other, friendsCount));  //mapkey输出用户和好友推介系数。
            context.write(new User(other, friendsCount), new User(key.toString(), friendsCount));  //好友关系是相互的，
        }
    }

    //同一时刻，同一个group的值同时处理，同一个group的值放在 Iterable arg1中
    static class SortReducer extends Reducer<User,User,Text,Text> {
        protected void reduce(User arg0, Iterable<User> arg1,
                              Context arg2)
                throws IOException, InterruptedException {
            String user = arg0.getName();       //用户名
            StringBuffer sb = new StringBuffer();
            for (User u : arg1) {
                sb.append(u.getName() + ":" + u.getFriendsCount() + ",");   //推介好友
            }
            arg2.write(new Text(user), new Text(sb.toString()));
        }
    }

}




