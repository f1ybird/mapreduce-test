package com.kevin.mapreduce.mr.demo;

import com.kevin.mapreduce.constants.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * describe  : 输出前三名的员工姓名和工资
 *
 * 输入文件：file/dept.txt file/emp.txt
 * 输出文件：file/salaryTop3Salary.txt
 *
 * 将输入文件上传至dfs的/input目录下，新建/output/out_dept/salaryTop3Salary为输出目录
 *
 * 运行方法：打成jar包在服务器上执行如下命令
 *
 * hadoop jar jar/mr.jar com.kevin.mapreduce.mr.demo.Q8SalaryTop3Salary /input/emp.txt /output/out_dept/salaryTop3Salary
 *
 * creat_user: kevin
 * creat_time: 2018/9/4 23:41
 * email     : kevin_love_it@163.com
 **/
public class Q8SalaryTop3Salary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable,Text, IntWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] kv = value.toString().split(Constant.COMMA_SPLIT);
            //输出key为0，value为员工姓名+“,”+员工工资
            context.write(new IntWritable(0),new Text(kv[1]+","+kv[5]));
        }
    }

    public static class ReduceClass extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            // 定义工资前三员工姓名
            String empName;
            String firstEmpName = "";
            String secondEmpName = "";
            String thirdEmpName = "";
            // 定义工资前三工资
            long empSalary = 0;
            long firstEmpSalary = 0;
            long secondEmpSalary = 0;
            long thirdEmpSalary = 0;
            // 通过冒泡法遍历所有员工，比较员工工资多少，求出前三名
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empSalary = Long.parseLong(val.toString().split(",")[1]);
                if (empSalary > firstEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = firstEmpName;
                    secondEmpSalary = firstEmpSalary;
                    firstEmpName = empName;
                    firstEmpSalary = empSalary;
                } else if (empSalary > secondEmpSalary) {
                    thirdEmpName = secondEmpName;
                    thirdEmpSalary = secondEmpSalary;
                    secondEmpName = empName;
                    secondEmpSalary = empSalary;
                } else if (empSalary > thirdEmpSalary) {
                    thirdEmpName = empName;
                    thirdEmpSalary = empSalary;
                }
            }
            // 输出工资前三名信息
            context.write(new Text("First employee name:" + firstEmpName), new Text("Salary:" + firstEmpSalary));
            context.write(new Text("Second employee name:" + secondEmpName), new Text("Salary:" + secondEmpSalary));
            context.write(new Text("Third employee name:" + thirdEmpName), new Text("Salary:" + thirdEmpSalary));
        }
    }

    public int run(String[] args) throws Exception {
        // 实例化作业对象，设置作业名称、 Mapper和Reduce类
        FileSystem fs = FileSystem.get(getConf());
        //设置job
        Job job = Job.getInstance(getConf());
        job.setJobName("Q8SalaryTop3Salary");
        job.setJarByClass(Q8SalaryTop3Salary.class);
        job.setMapperClass(Q8SalaryTop3Salary.MapClass.class);
        job.setReducerClass(Q8SalaryTop3Salary.ReduceClass.class);
        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        // 第1个参数员工数据路径和第2个参数为输出路径
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

    public static void main(String[] args) throws Exception{
        int result = ToolRunner.run(new Configuration(), new Q8SalaryTop3Salary(), args);
        System.exit(result);
    }
}
