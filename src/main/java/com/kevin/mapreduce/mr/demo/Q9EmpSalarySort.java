package com.kevin.mapreduce.mr.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * describe  : 将全体员工按照总收入（工资+提成）从高到低排列
 *
 * 输入文件：file/dept.txt file/emp.txt
 * 输出文件：file/empSalarySort.txt
 *
 * 将输入文件上传至dfs的/input目录下，新建/output/out_dept/empSalarySort为输出目录
 *
 * 运行方法：打成jar包在服务器上执行如下命令
 *
 * hadoop jar jar/mr.jar com.kevin.mapreduce.mr.demo.Q9EmpSalarySort /input/emp.txt /output/out_dept/empSalarySort
 *
 * creat_user: kevin
 * creat_time: 2018/9/4 23:58
 * email     : kevin_love_it@163.com
 **/
public class Q9EmpSalarySort extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            // 对员工文件字段进行拆分
            String[] kv = value.toString().split(",");
            // 输出key为员工所有工资和value为员工姓名
            int empAllSalary = "".equals(kv[6]) ? Integer.parseInt(kv[5]) : Integer.parseInt(kv[5]) + Integer.parseInt(kv[6]);
            context.write(new IntWritable(empAllSalary), new Text(kv[1]));
        }

        /**
         * 递减排序算法
         */
        public static class DecreaseComparator extends IntWritable.Comparator {
            public int compare(WritableComparable a, WritableComparable b) {
                return -super.compare(a, b);
            }
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        //设置job
        Job job = Job.getInstance(getConf());
        job.setJobName("Q9EmpSalarySort");
        // 设置Mapper和Reduce类
        job.setJarByClass(Q9EmpSalarySort.class);
        job.setMapperClass(MapClass.class);
        // 设置输出格式类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(MapClass.DecreaseComparator.class);
        // 第1个参数为员工数据路径和第2个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(),args).getRemainingArgs();
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        Path outPath = new Path(otherArgs[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }
    /**
     * 主方法，执行入口
     * @param args 输入参数
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q9EmpSalarySort(), args);
        System.exit(res);
    }
}
