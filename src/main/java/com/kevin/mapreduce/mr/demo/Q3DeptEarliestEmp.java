package com.kevin.mapreduce.mr.demo;

import com.kevin.mapreduce.constants.Constant;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * describe  : 计算各个部门的人数和平均工资(类似于求各个部门总工资，只需在reduce阶段额外计算部门人数即可)
 * <p>
 * 输入文件：file/dept.txt file/emp.txt
 * 输出文件：file/deptEarliestEmp.txt
 * <p>
 * 将输入文件上传至dfs的/input目录下，新建/output/out_dept/deptEarliestEmp为输出目录
 * <p>
 * 运行方法：打成jar包在服务器上执行如下命令
 * <p>
 * hadoop jar jar/mr.jar com.kevin.mapreduce.mr.demo.Q3DeptEarliestEmp /input/dept.txt /input/emp.txt /output/out_dept/deptEarliestEmp
 * <p>
 * creat_user: kevin
 * creat_time: 2018/9/3 23:05
 * email     : kevin_love_it@163.com
 **/
public class Q3DeptEarliestEmp extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        //用于缓存dept中的数据
        private Map<String, String> deptMap = new HashMap<>();
        private String[] kv;

        /**
         * 此方法会在mapper之前执行且执行一次
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) {
            BufferedReader br = null;
            try {
                //从当前作业中获取要缓存的文件
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                for (Path path : paths) {
                    //对部门文件字段进行拆分并缓存到deptMap中
                    if (path.toString().contains("dept")) {
                        br = new BufferedReader(new FileReader(path.toString()));
                        String deptIdName = "";
                        while (null != ((deptIdName = br.readLine()))) {
                            // 对部门文件字段进行拆分并缓存到deptMap中,其中Map中key为部门编号， value为所在部门名称
                            deptMap.put(deptIdName.split(Constant.COMMA_SPLIT)[0], deptIdName.split(Constant.COMMA_SPLIT)[1]);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != br) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            kv = value.toString().split(Constant.COMMA_SPLIT);
            // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工姓名+入职时间
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[4] && !"".equals(kv[4].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[1].trim() + "," + kv[4].trim()));
                }
            }

        }
    }

    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 员工姓名和进入公司日期
            String empName = null;
            String empEnterDate = null;
            // 设置日期转换格式和最早进入公司的员工、日期
            DateFormat df = new SimpleDateFormat("dd-MM月-yy");
            Date earliestDate = new Date();
            String earliestEmp = null;
            // 遍历该部门下所有员工，得到最早进入公司的员工信息
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empEnterDate = val.toString().split(",")[1].toString().trim();
                try {
                    System.out.println(df.parse(empEnterDate));
                    if (df.parse(empEnterDate).compareTo(earliestDate) < 0) {
                        earliestDate = df.parse(empEnterDate);
                        earliestEmp = empName;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            // 输出key为部门名称和value为该部门最早进入公司员工
            context.write(key, new Text("The earliest emp of dept:" + earliestEmp + ", Enter date:" + new SimpleDateFormat("yyyy-MM-dd").format(earliestDate)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 实例化作业对象，设置作业名称、 Mapper和Reduce类
        FileSystem fs = FileSystem.get(getConf());
        //设置job
        Job job = Job.getInstance(getConf());
        job.setJobName("Q3DeptEarliestEmp");
        job.setJarByClass(Q3DeptEarliestEmp.class);
//        job.setJar(PathConst.JAR_OUTPUT_PATH);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        // 设置输入格式类
        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出格式
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径
        String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
        DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        Path outPath = new Path(otherArgs[2]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new Q3DeptEarliestEmp(), args);
        System.exit(result);
    }
}
