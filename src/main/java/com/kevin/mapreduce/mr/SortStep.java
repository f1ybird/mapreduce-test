package com.kevin.mapreduce.mr;

import com.kevin.mapreduce.constants.Constant;
import com.kevin.mapreduce.utils.MapReduceJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * describe  : 排序
 *
 * 输入文件：file/sum_sumStep.txt
 * 输出文件：file/out_sortStep.txt
 *
 * creat_user: zhangkai
 * creat_time: 2018/8/26 22:09
 * email     : kevin_love_it@163.com
 **/
public class SortStep {

    /**
     * 抽取公共代码作为工具类创建任务
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        Job job = MapReduceJobUtil.buildJob(new Configuration(), args, SortStep.class,
                TextInputFormat.class, SortMapper.class, InfoBean.class, NullWritable.class,
                TextOutputFormat.class, SortReducer.class, Text.class, InfoBean.class,null);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * 按照步骤创建任务
     * @param args
     * @throws Exception
     */
    public static void main1(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        //job
        Job job = Job.getInstance(conf, "sort step");
        job.setJarByClass(SortStep.class);

        //mapper
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //reducer
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // 该段代码是用来判断输出路径存在不存在，存在就删除，虽然方便操作，但请谨慎
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        if(fs.isDirectory(outputPath)){
            fs.delete(outputPath,true);
        }

        FileOutputFormat.setOutputPath(job,outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    /**
     * sort mapper
     */
    public static class SortMapper extends Mapper<LongWritable, Text,InfoBean, NullWritable>{

        private InfoBean bean = new InfoBean();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(Constant.TAB_SPLIT_4);
            if(fields != null && fields.length > 0){
                String account = fields[0];
                double income = Double.parseDouble(fields[1]);
                double expenses = Double.parseDouble(fields[2]);
                bean.set(account,income,expenses);
                context.write(bean,NullWritable.get());
            }
        }
    }

    /**
     * sort reduce
     */
    public static class SortReducer extends Reducer<InfoBean, NullWritable,Text,InfoBean>{

        private Text keyText = new Text();

        protected void reduce(InfoBean bean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            keyText.set(bean.getAccount());
            context.write(keyText,bean);
        }
    }
}
