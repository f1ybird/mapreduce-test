package com.kevin.mapreduce.mr;

import com.kevin.mapreduce.constants.Constant;
import com.kevin.mapreduce.utils.MapReduceJobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * describe  : 倒排索引 单词-文档矩阵，某个关键词在文档中出现的次数
 *
 * 输入文件：
 * file/in_inverseIndex01.txt
 * file/in_inverseIndex02.txt
 *
 * 输出文件：
 * file/out_inverseIndex.txt
 *
 * creat_user: zhangkai
 * creat_time: 2018/8/27 23:29
 * email     : kevin_love_it@163.com
 **/
public class InverseIndex {

    public static void main(String[] args) throws Exception {

        Job job = MapReduceJobUtil.buildJob(new Configuration(), args, InverseIndex.class,
                TextInputFormat.class, IndexMapper.class, Text.class, Text.class,
                TextOutputFormat.class, IndexReduce.class, Text.class, Text.class, IndexCombiner.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(Constant.TAB_SPLIT_4);
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            Path path = inputSplit.getPath();
            String name = path.getName();
            for (String f : fields) {
                k.set(f + "->" + name);
                v.set("1");
                context.write(k, v);
            }
        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {

        private Text k = new Text();
        private Text v = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] fields = key.toString().split("->");
            long sum = 0;
            for (Text t : values) {
                sum += Long.parseLong(t.toString());
            }
            k.set(fields[0]);
            v.set(fields[1] + "->" + sum);
            context.write(k, v);
        }
    }

    public static class IndexReduce extends Reducer<Text, Text, Text, Text> {

        private Text v = new Text();

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String value = "";
            for (Text t : values) {
                value += t.toString() + " ";
            }
            v.set(value);
            context.write(key, v);
        }
    }
}
