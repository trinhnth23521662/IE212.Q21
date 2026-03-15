import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeGroupMovieRating {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{

        String filename;

        protected void setup(Context context){
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String line = value.toString();

            if(line.trim().isEmpty()) return;

            String[] p = line.split(", ");

            try{

                if(filename.contains("users")){

                    if(p.length >= 3){
                        String userID = p[0];
                        String age = p[2];

                        context.write(new Text("U"+userID), new Text(age));
                    }
                }

                else if(filename.contains("ratings")){

                    if(p.length >= 3){
                        String userID = p[0];
                        String movieID = p[1];
                        String rating = p[2];

                        context.write(new Text("U"+userID),
                                new Text("R|"+movieID+"|"+rating));
                    }
                }

                else if(filename.contains("movies")){

                    if(p.length >= 2){
                        String movieID = p[0];
                        String title = p[1];

                        context.write(new Text("M"+movieID),
                                new Text(title));
                    }
                }

            }catch(Exception e){}
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{

        HashMap<String,String> movieTitle = new HashMap<>();
        HashMap<String,HashMap<String,Double>> sum = new HashMap<>();
        HashMap<String,HashMap<String,Integer>> count = new HashMap<>();

        TreeMap<String,String> sortedOutput = new TreeMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context){

            String k = key.toString();

            try{

                if(k.startsWith("M")){

                    for(Text v:values)
                        movieTitle.put(k.substring(1), v.toString());
                }

                else if(k.startsWith("U")){

                    int age = -1;
                    List<String> ratings = new ArrayList<>();

                    for(Text v:values){

                        String val = v.toString();

                        if(val.startsWith("R|"))
                            ratings.add(val);
                        else
                            age = Integer.parseInt(val);
                    }

                    if(age==-1) return;

                    String group;

                    if(age<=18) group="0-18";
                    else if(age<=35) group="18-35";
                    else if(age<=50) group="35-50";
                    else group="50+";

                    for(String r:ratings){

                        String[] parts = r.split("\\|");

                        String movie = parts[1];
                        double rating = Double.parseDouble(parts[2]);

                        sum.putIfAbsent(movie,new HashMap<>());
                        count.putIfAbsent(movie,new HashMap<>());

                        HashMap<String,Double> s = sum.get(movie);
                        HashMap<String,Integer> c = count.get(movie);

                        s.put(group, s.getOrDefault(group,0.0)+rating);
                        c.put(group, c.getOrDefault(group,0)+1);
                    }
                }

            }catch(Exception e){}
        }

        protected void cleanup(Context context)
                throws IOException,InterruptedException{

            String[] groups = {"0-18","18-35","35-50","50+"};

            for(String movie:sum.keySet()){

                String title = movieTitle.getOrDefault(movie,movie);

                HashMap<String,Double> s = sum.get(movie);
                HashMap<String,Integer> c = count.get(movie);

                String out="";

                for(String g:groups){

                    out += g + ": ";

                    if(!c.containsKey(g))
                        out += "NA\t";
                    else{

                        double avg = s.get(g)/c.get(g);
                        out += String.format("%.2f",avg)+"\t";
                    }
                }

                sortedOutput.put(title,out);
            }

            for(Map.Entry<String,String> entry : sortedOutput.entrySet()){
                context.write(new Text(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"AgeGroupMovieRating");

        job.setJarByClass(AgeGroupMovieRating.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for(int i=0;i<args.length-1;i++)
            FileInputFormat.addInputPath(job,new Path(args[i]));

        FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
