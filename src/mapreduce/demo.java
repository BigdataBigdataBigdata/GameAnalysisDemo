package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


import com.google.gson.JsonParseException;

 
public class demo {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

    	private static final Log log = LogFactory.getLog(TokenCounterMapper.class);
    	private final static Text one = new Text();
    	private Text word = new Text();
    	    
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

//        	try{
//
//                JSONObject jsn = new JSONObject(value.toString());
//                String text = (String) jsn.get("text");
//                
//                log.info("user_id");
//                log.info(text);
//                StringTokenizer itr = new StringTokenizer(text);
//                while (itr.hasMoreTokens()) {
//                    word.set(itr.nextToken());
//                    context.write(word, one);
//                }
//        		
//            }catch(JsonParseException e){
//                e.printStackTrace();
//            } catch (JSONException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
    		
            String author;
            String book;
            String line = value.toString();
            String[] tuple = line.split("\\n");
            try{
                for(int i=0;i<tuple.length; i++){
                    JSONObject obj = new JSONObject(tuple[i]);
                    author = obj.getString("author");              
                    book = obj.getString("book");
                    context.write(new Text(author), new Text(book));
                }
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }

//    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//        public void reduce(Text key, Iterator<IntWritable> values, Context context) 
//          throws IOException, InterruptedException {
//            int sum = 0;
//            while (values.hasNext()) {
//                sum += values.next().get();
//            }
//            context.write(key, new IntWritable(sum));
//        }
//     }

    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            try{
                JSONObject obj = new JSONObject();
                JSONArray ja = new JSONArray();
                for(Text val : values){
                    JSONObject jo = new JSONObject().put("Test", val.toString());
                    ja.put(jo);
                }
                obj.put("author", ja);
                obj.put("book", key.toString());
                context.write(NullWritable.get(), new Text(obj.toString()));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: GameLogAnalysis <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "demo");
//        job.setJarByClass(demo.class);
//             
//             job.setOutputKeyClass(Text.class);
//             job.setOutputValueClass(IntWritable.class);
//                 
//             job.setMapperClass(Map.class);
//             job.setReducerClass(Reduce.class);
//                 
//             job.setInputFormatClass(TextInputFormat.class);
//             job.setOutputFormatClass(TextOutputFormat.class);
//                 
//             FileInputFormat.addInputPath(job, new Path(args[0]));
//             FileOutputFormat.setOutputPath(job, new Path(args[1]));
//                 
//             job.waitForCompletion(true);
        
        job.setJarByClass(demo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}