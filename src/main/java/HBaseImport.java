import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by zzy on 15/11/17.
 */

/**
 * mr 中操作hbase
 */
public class HBaseImport {
    static class BatchImportMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            String line = value.toString();
            String[] splited = line.split("\t");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            String time =sdf.format(new Date(Long.parseLong(splited[0].trim())));

            String rowkey = splited[1]+"_"+time;
            Text v2s = new Text();
            v2s.set(rowkey+"\t" +line);
            context.write(key,v2s);

        }
    }
    static class BatchImportReducer extends TableReducer<LongWritable,Text,NullWritable> {
        private byte[] family = "cf".getBytes();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
            for (Text v2 :v2s){
                String [] splited = v2.toString().split("\t");
                String rowKey = splited[0];
                Put put =  new Put(rowKey.getBytes());
                put.add(family,"raw".getBytes(),v2.toString().getBytes());
                put.add(family,"reportTime".getBytes(),splited[1].getBytes());
                put.add(family,"msisdn".getBytes(),splited[2].getBytes());
                put.add(family,"apmac".getBytes(),splited[3].getBytes());
                put.add(family,"acmac".getBytes(),splited[4].getBytes());
            }
        }
    }
    private static final String tableName = "logs";
    private  static  void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","192.168.122.213:2181");
        conf.set("hbase.rootdir","hdfs://192.168.122.211:9000/hbase");
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

        Job job = Job.getInstance(conf,HBaseImport.class.getSimpleName());

        TableMapReduceUtil.addDependencyJars(job);
        job.setJarByClass(HBaseImport.class);
        job.setMapperClass(BatchImportMapper.class);
        job.setReducerClass(BatchImportReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.setInputPaths(job, "hdfs://192.168.122.211:9000/user/hbase");
        job.waitForCompletion(true);
//        FileInputFormat.setInputPaths(job,"");

    }
}
