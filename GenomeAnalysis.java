import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class GenomeAnalysis {

  public static void main(String[] args) {
    if (args.length != 5) {
        System.err.println("Usage: GenomeAnalysis <input path> <gene path> <output path> <Map Task Num> <Reduce Task Num>");
        System.exit(-1);
    }

    JobClient client = new JobClient();
    JobConf conf = new JobConf(GenomeAnalysis.class);
    conf.set("genePath", args[1]);
    // specify output types
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // specify input and output dirs
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    // specify a mapper
    conf.setMapperClass(GenomeAnalysisMapper.class);

    // specify a reducer
    conf.setReducerClass(GenomeAnalysisReducer.class);
    conf.setCombinerClass(GenomeAnalysisReducer.class);

    try {
        conf.setNumMapTasks(Integer.parseInt(args[3]));
        conf.setNumReduceTasks(Integer.parseInt(args[4]));    

        client.setConf(conf);
        JobClient.runJob(conf);
    } catch (Exception e) {
        e.printStackTrace();
    }
  }
}
