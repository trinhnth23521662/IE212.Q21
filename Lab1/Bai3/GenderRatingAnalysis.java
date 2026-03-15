import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderRatingAnalysis {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        String fileName;

        protected void setup(Context context) throws IOException, InterruptedException {
            fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",\\s*");

            if (fileName.contains("users")) {

                String userID = parts[0];
                String gender = parts[1];

                context.write(new Text("U" + userID), new Text("G," + gender));

            } else if (fileName.contains("ratings")) {

                String userID = parts[0];
                String movieID = parts[1];
                String rating = parts[2];

                context.write(new Text("U" + userID),
                        new Text("R," + movieID + "," + rating));

            } else if (fileName.contains("movies")) {

                String movieID = parts[0];
                String title = parts[1];

                context.write(new Text("M" + movieID),
                        new Text(title));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        Map<String,String> movieMap = new HashMap<>();
        Map<String,double[]> ratingMap = new HashMap<>();

        TreeMap<String,String> sortedOutput = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String k = key.toString();

            if (k.startsWith("M")) {

                for (Text v : values) {
                    movieMap.put(k.substring(1), v.toString());
                }

            } else if (k.startsWith("U")) {

                String gender = "";
                List<String[]> ratings = new ArrayList<>();

                for (Text v : values) {

                    String val = v.toString();

                    if (val.startsWith("G")) {

                        gender = val.split(",")[1];

                    } else if (val.startsWith("R")) {

                        ratings.add(val.split(","));
                    }
                }

                for (String[] r : ratings) {

                    String movieID = r[1];
                    double rating = Double.parseDouble(r[2]);

                    double[] data = ratingMap.getOrDefault(movieID,new double[4]);

                    if (gender.equals("M")) {
                        data[0]+=rating;
                        data[1]++;
                    } else {
                        data[2]+=rating;
                        data[3]++;
                    }

                    ratingMap.put(movieID,data);
                }
            }
        }

        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (String movieID : ratingMap.keySet()) {

                double[] d = ratingMap.get(movieID);

                double maleAvg = d[1]==0?0:d[0]/d[1];
                double femaleAvg = d[3]==0?0:d[2]/d[3];

                String title = movieMap.get(movieID);

                if(title==null) title = movieID;

                String output =
                        "Male: "+String.format("%.2f",maleAvg)
                        +", Female: "+String.format("%.2f",femaleAvg);

                sortedOutput.put(title, output);
            }

            for(Map.Entry<String,String> entry : sortedOutput.entrySet()){
                context.write(new Text(entry.getKey()),
                        new Text(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Gender Rating Analysis");

        job.setJarByClass(GenderRatingAnalysis.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for(int i=0;i<args.length-1;i++){
            FileInputFormat.addInputPath(job,new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
