import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class ToolWordCount extends Configured implements Tool {
	
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ToolWordCount(), args);
    System.exit(res);
  }	

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
      word.set("MAP_RUN");
      context.write(word, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private final static IntWritable one = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
      context.write(new Text("Reducer"), one);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
	BasicConfigurator.configure();
	  
    Configuration conf = this.getConf();
    conf.addResource(new Path("C:\\home\\app\\hadoop-2.7.2\\etc\\hadoop\\core-site.xml"));
    conf.addResource(new Path("C:\\home\\app\\hadoop-2.7.2\\etc\\hadoop\\hdfs-site.xml"));
    
    //conf.set("hadoop.home.dir", "/opt/hadoop-2.7.2");
    Job job = Job.getInstance(conf, ToolWordCount.class.getCanonicalName());
    
    job.setJarByClass(ToolWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //FileInputFormat.addInputPath(job, new Path(args[0]));
    //FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path("/user/kungy/input"));
    FileOutputFormat.setOutputPath(job, new Path("/user/kungy/output"));
    return (job.waitForCompletion(true)) ? 0 : 1;
  }
}