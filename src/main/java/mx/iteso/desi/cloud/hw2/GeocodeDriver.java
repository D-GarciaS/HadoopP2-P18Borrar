package mx.iteso.desi.cloud.hw2;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import mx.iteso.desi.cloud.GeocodeWritable;
import mx.iteso.desi.cloud.Triple;

public class GeocodeDriver {



  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: GeocodeDriver <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJarByClass(GeocodeDriver.class);
    job.setMapperClass(GeocodeMapper.class);
    // job.setCombinerClass(GeocodeReducer.class);
    job.setReducerClass(GeocodeReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(GeocodeWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputKeyClass(Text.class);
    //job.addCacheArchive(new Path(args[0]+"/geo").toUri());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
