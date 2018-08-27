package com.kevin.mapreduce.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词统计
 *
 * 输入：
 *  hello tom
 *  hello tom2
 *  hello tom3
 *  hello tom4
 *  hello tom5
 *  输出：
 *  hello   5
 *  tom     1
 *  tom2    1
 *  tom3    1
 *  tom4    1
 *  tom5    1
 *
 * @author fengmingyue
 */
public class WordCountDemo2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);
//        job.setJarByClass(WordCountDemo2.class);
        job.setJar("out/artifacts/wordCount/wordCount.jar");
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        Path inputPath = new Path("/input/words.txt");
        FileInputFormat.setInputPaths(job, inputPath);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        Path outputPath = new Path("/output");
        //如果输出目录已存在，则删除
        if(fs.isDirectory(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job,outputPath );
        /**
         * Combiner的输出是Reducer的输入，如果Combiner是可插拔的，添加Combiner绝不能改变最终的计算结果。
         * 所以Combiner只应该用于那种Reduce的输入key/value与输出key/value类型完全一致，且不影响最终结果的场景。
         * 比如累加，最大值等。
         */
        job.setCombinerClass(WCReducer.class);
        job.waitForCompletion(true);
    }
}
class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long counter = 0;
        for(LongWritable l : values){
            counter += l.get();
        }
        context.write(key, new LongWritable(counter));
    }
}
class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for(String w : words){
            context.write(new Text(w), new LongWritable(1));
        }
    }
}