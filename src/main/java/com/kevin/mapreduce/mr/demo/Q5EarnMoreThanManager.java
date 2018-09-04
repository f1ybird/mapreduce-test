package com.kevin.mapreduce.mr.demo;

import com.kevin.mapreduce.constants.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * describe  : 列出工资比上司工资高的员工姓名及其工资
 *
 * 输入文件：file/dept.txt file/emp.txt
 * 输出文件：file/earnMoreThanManager.txt
 *
 * creat_user: kevin
 * creat_time: 2018/9/4 21:51
 * email     : kevin_love_it@163.com
 **/

public class Q5EarnMoreThanManager extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //对员工字段进行拆分
            String[] kv = value.toString().split(Constant.COMMA_SPLIT);
            //输出经理数据：key为经理编号，value为"M,"+经理工资
            context.write(new Text(kv[0].toString()),new Text("M" +","+ kv[5]));

            if(StringUtils.isNotBlank(kv[3])){
                //输出经理对应员工数据：key为经理编号，value为"E,"+员工姓名+员工工资
                context.write(new Text(kv[3]),new Text("E"+","+kv[1]+","+kv[5]));
            }
        }
    }

    public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, Long> empMap = new HashMap<>();
            long mgrSalary = 0L;

            for (Text t : values){
                //保存员工姓名及其工资
                if(t.toString().startsWith("E")){
                    String[] emp = t.toString().split(Constant.COMMA_SPLIT);
                    empMap.put(emp[1],Long.parseLong(emp[2]));
                //保存经理姓名及其工资
                }else{
                    mgrSalary = Long.parseLong(t.toString().split(Constant.COMMA_SPLIT)[1]);
                }
            }

            for (Map.Entry<String, Long> entry : empMap.entrySet()){
                if(entry.getValue() > mgrSalary){
                    context.write(new Text(entry.getKey()),new Text(entry.getValue()+""));
                }
            }

        }
    }


    @Override
    public int run(String[] args) throws Exception {
        // 实例化作业对象，设置作业名称、 Mapper和Reduce类
        FileSystem fs = FileSystem.get(getConf());
        //设置job
        Job job = Job.getInstance(getConf());
        job.setJobName("Q5EarnMoreThanManager");
        job.setJarByClass(Q5EarnMoreThanManager.class);
//        job.setJar(PathConst.JAR_OUTPUT_PATH);
        job.setMapperClass(Q5EarnMoreThanManager.MapClass.class);
        job.setReducerClass(Q5EarnMoreThanManager.ReduceClass.class);
        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),args).getRemainingArgs();
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(),job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        Path outPath = new Path(otherArgs[2]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception{
        int result = ToolRunner.run(new Configuration(), new Q5EarnMoreThanManager(), args);
        System.exit(result);
    }
}
