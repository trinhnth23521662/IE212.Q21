import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class GenreRatingAverage {

    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, String> movieGenres = new HashMap<>();

        protected void setup(Context context) throws IOException {

            BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
            String line;

            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",", 3);

                if (parts.length == 3) {

                    String movieId = parts[0].trim();
                    String genres = parts[2].trim();

                    movieGenres.put(movieId, genres);
                }
            }

            br.close();
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("UserID"))
                return;

            String[] parts = line.split(",");

            if (parts.length >= 3) {

                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                String genres = movieGenres.get(movieId);

                if (genres != null) {

                    String[] genreList = genres.split("\\|");

                    for (String genre : genreList) {

                        context.write(new Text(genre), new Text(rating + ",1"));
                    }
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (Text val : values) {

                String[] parts = val.toString().split(",");

                sum += Double.parseDouble(parts[0]);
                count += Integer.parseInt(parts[1]);
            }

            double avg = sum / count;

            String avgFormatted = String.format("%.2f", avg);

            context.write(key,
                    new Text("Avg: " + avgFormatted + ",    Count: " + count));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 3) {
            System.err.println("Usage: GenreRatingAverage <ratings1> <ratings2> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Genre Average Rating");

        job.setJarByClass(GenreRatingAverage.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
