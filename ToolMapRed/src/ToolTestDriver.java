
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ToolTestDriver extends Configured implements Tool {
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ToolTestDriver(), args);
        System.exit(res);
    }

    @Override
	public int run(String[] args) throws Exception {
		//Configuration conf = new Configuration();
    	Configuration conf = this.getConf();
    	
		Job job = Job.getInstance(conf, ToolTestDriver.class.getCanonicalName());
		job.setJarByClass(ToolTestDriver.class);
		
		// TODO: specify a mapper
		job.setMapperClass(Mapper.class);
		// TODO: specify a reducer
		job.setReducerClass(Reducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("in"));
		FileOutputFormat.setOutputPath(job, new Path("out"));

		return (job.waitForCompletion(true)) ? 0 : 1;
	}

}
