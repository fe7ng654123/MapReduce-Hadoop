import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramRF{

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MapWritable>{
            
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
                Double theta = Double.parseDouble(context.getConfiguration().get("THETA"));
                MapWritable final_map_output = new MapWritable();  //<Text(string head), MapWritable(mini-stripe)>
                MapWritable stripe = new MapWritable();
                StringBuffer str_head = new StringBuffer();
                StringBuffer str_sub = new StringBuffer();               
				for (int i = 0; i < ls.size() - (n-1); i++) {
					int k=i;
					for(int j=0;j<n;j++) { 
						if(j == 0){
                            str_head = str_head.append(ls.get(k));
                            
                            if(!final_map_output.containsKey(new Text(str_head.toString()))){
                                final_map_output.put(new Text(str_head.toString()), new MapWritable());
                            }
                        }
                        else if(j == 1){
                            str_sub = str_sub.append(ls.get(k));
                        }
                        else {
                            str_sub = str_sub.append(" ");
							str_sub = str_sub.append(ls.get(k));
                        }
						k++;
                    }
                    
                    word.set(str_head.toString());
                    stripe.put(new Text(str_sub.toString()), one);
                    stripe.put(new Text("*"), one);
                    str_head.delete(0, str_head.length());
                    str_sub.delete(0, str_sub.length());
					
                    
                    MapWritable word_map = (MapWritable) final_map_output.get(word);
                    for (Writable stripe_key : stripe.keySet()) {                       
                        int map1val = ((IntWritable) word_map.getOrDefault(stripe_key, new IntWritable(0))).get();
                        word_map.put(stripe_key, new IntWritable(map1val + 1));                       
                    }

                    stripe.clear();
                }
                
                for (Writable word_key : final_map_output.keySet()){
                    Text output_word = new Text(word_key.toString());
                    context.write(output_word, (MapWritable) final_map_output.get(word_key));
                }

			}
    }



    public static class IntSumReducer
            extends Reducer<Text,MapWritable,Text,DoubleWritable> {
            private MapWritable result = new MapWritable();

            public void reduce(Text key, Iterable<MapWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
                Double theta = Double.parseDouble(context.getConfiguration().get("THETA"));
                
                for (MapWritable val : values) {
                    
                    for (Writable mapkey : val.keySet()) {
                        
                        int map1val = ((IntWritable) result.getOrDefault(mapkey, new IntWritable(0))).get();
                        int map2val = ((IntWritable) val.get(mapkey)).get();
                        result.put(mapkey, new IntWritable(map1val + map2val));
                        
                    }

                    
                }
                
                IntWritable test = (IntWritable) result.get(new Text("*"));
                double sum_substring = (double)test.get();
                for (Writable mapkey : result.keySet()) {
                    
                    if(!mapkey.toString().equals("*")){
                        double frequency = ((IntWritable)result.get(mapkey)).get()/sum_substring;
                        if(frequency >= theta){
                            String full_string = key.toString() + " " + mapkey.toString();
                            context.write(new Text(full_string), new DoubleWritable(frequency));
                        }
                    }
                }
                result.clear();


                    }
    }
	
	

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		conf.set("N", args[2]);
        conf.set("THETA",args[3]);
        Job job = Job.getInstance(conf, "N gram RF");
        job.setJarByClass(NgramRF.class);
        job.setMapperClass(TokenizerMapper.class);
        
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
