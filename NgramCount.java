import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount{

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
			List ls = new ArrayList();
			@SuppressWarnings("unchecked")
            public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
					String[] stringArray = itr.nextToken().split("\\W+");
					for( String s : stringArray){
						if(s.length() != 0)	
							ls.add(s);
					}
                }
			}
					
			@Override
			protected void cleanup(Context context) throws IOException, InterruptedException {
				int n = Integer.parseInt(context.getConfiguration().get("N"));
				StringBuffer str = new StringBuffer();
				for (int i = 0; i < ls.size() - (n-1); i++) {
					int k=i;
					for(int j=0;j<n;j++) { 
						if(j>0) {
							str = str.append(" ");
							str = str.append(ls.get(k));
						} 
						else 
						{
							str = str.append(ls.get(k));
						}
						k++;
					}
					word.set(str.toString());
					str.delete(0, str.length());
					// one.set(ls.size());
					context.write(word, one);
				}
			}
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result = new IntWritable();

            public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
                    }
    }
	


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		conf.set("N", args[2]);
		conf.set("mapreduce.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "N gram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
