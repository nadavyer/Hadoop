
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step11Last {
    public static class MapperClass extends Mapper<LongWritable, Text, LastKey, Text> {
        private Text _val = new Text("");
        private LastKey wrappedKey = new LastKey();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                String[] parts = itr.nextToken().split("\t");
                String[] words = parts[0]. split(" ");
                if (words.length != 3) {
                    continue;
                }
                wrappedKey.set(new Text(parts[0] + " " + parts[1])); //val to key

                context.write(wrappedKey, _val);
            }
        }
    }

    public static class ReducerClass extends Reducer<LastKey,Text,LastKey,Text> {
        @Override
        public void reduce(LastKey key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {

            context.write(key, new Text(""));
        }
    }




    public static class PartitionerClass extends Partitioner<LastKey, Text> {
        private LastKey _key = new LastKey();
        @Override
        public int getPartition(LastKey key, Text value, int numPartitions) {
            _key.set(key.getData());
            return (_key.getArgsToHash().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 11Last");
        job.setJarByClass(Step11Last.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(LastKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LastKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}