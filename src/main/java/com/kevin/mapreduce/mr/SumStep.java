package com.kevin.mapreduce.mr;

import com.kevin.mapreduce.constants.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 先求和，再排序
 *
 * @author fengmingyue
 */
public class SumStep {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sum step");
        job.setJarByClass(SumStep.class);
        job.setMapperClass(SumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);

        job.setReducerClass(SumReducer.class);
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

    public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private InfoBean bean = new InfoBean();
        private Text k = new Text();
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(Constant.TAB_SPLIT_4);
            String account = fields[0];
            double income = Double.parseDouble(fields[1]);
            double expenses = Double.parseDouble(fields[2]);
            k.set(account);
            bean.set(account, income, expenses);
            context.write(k, bean);
        }
    }
    public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {
        private InfoBean bean = new InfoBean();
        protected void reduce(Text key, Iterable<InfoBean> v2s, Context context)
                throws IOException, InterruptedException {
            double in_sum = 0;
            double out_sum = 0;
            for(InfoBean bean : v2s){
                in_sum += bean.getIncome();
                out_sum += bean.getExpenses();
            }
            bean.set("", in_sum, out_sum);
            context.write(key, bean);
        }
    }
}
class InfoBean implements WritableComparable<InfoBean> {
    private String account;
    private double income;
    private double expenses;
    private double surplus;
    public void set(String account, double income, double expenses){
        this.account = account;
        this.income = income;
        this.expenses = expenses;
        this.surplus = income - expenses;
    }
    public String toString() {
        return this.income + "\t" + this.expenses + "\t" + this.surplus;
    }
    //serialize
    public void write(DataOutput out) throws IOException {
        out.writeUTF(account);
        out.writeDouble(income);
        out.writeDouble(expenses);
        out.writeDouble(surplus);
    }
    public void readFields(DataInput in) throws IOException {
        this.account = in.readUTF();
        this.income = in.readDouble();
        this.expenses = in.readDouble();
        this.surplus = in.readDouble();
    }
    public int compareTo(InfoBean o) {
        if(this.income == o.getIncome()){
            return this.expenses > o.getExpenses() ? 1 : -1;
        } else {
            return this.income > o.getIncome() ? -1 : 1;
        }
    }
    public String getAccount() {
        return account;
    }
    public void setAccount(String account) {
        this.account = account;
    }
    public double getIncome() {
        return income;
    }
    public void setIncome(double income) {
        this.income = income;
    }
    public double getExpenses() {
        return expenses;
    }
    public void setExpenses(double expenses) {
        this.expenses = expenses;
    }
    public double getSurplus() {
        return surplus;
    }
    public void setSurplus(double surplus) {
        this.surplus = surplus;
    }
}
