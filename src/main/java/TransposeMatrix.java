import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransposeMatrix {

    // Mapper
    public static class CellMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            int i = 0;
            String content;
            String positionContent;
            StringTokenizer itr = new StringTokenizer(value.toString());
            // We write a key/value per matrix cell with: key=csv_column_index, value="csv_line_index,cell_content"
            while (itr.hasMoreTokens()) {
                content = itr.nextToken(",");
                positionContent = Long.toString(key.get()) + "," + content;
                System.out.println(positionContent);
                context.write(new IntWritable(i), new Text(positionContent));
                i++;
            }
        }
    }

    //The shuffle & sort step gathers cell contents by column_index (which is the future line_index)

    //Reducer
    public static class LineReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            Text result = new Text();
            String line = "";

            // We get all the "index,cell_content" for the line & order them
            // They will be ordered by the ex line_index which is the new column_index
            ArrayList<String> orderedLine = new ArrayList<String>();
            for (Text val : values) {
                System.out.println(val.toString());
                orderedLine.add(val.toString());
            }
            Collections.sort(orderedLine);
            // We get the cell contents and concatenate them in a single string separated by comas
            for (String t : orderedLine) {
                if(line != ""){
                    line += ",";
                }
                line += t.split(",")[1];
            }
            result.set(line);
            // We write a key/value with: key=csv_line_index, value=csv_line_content
            context.write(key, result);
        }
    }

    // Output format config
    public static class CsvOutputFormat extends FileOutputFormat<IntWritable, Text> {
        public CsvOutputFormat(){
            super();
        }

        @Override
        public org.apache.hadoop.mapreduce.RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext arg0)
                throws IOException, InterruptedException {
            Path path = FileOutputFormat.getOutputPath(arg0);
            Path fullPath = new Path(path, "output.csv");

            //create the file in the file system
            FileSystem fs = path.getFileSystem(arg0.getConfiguration());
            FSDataOutputStream fileOut = fs.create(fullPath, arg0);

            //create our record writer with the new file
            return new CsvRecordWriter(fileOut);
        }
    }

    // Output writer config
    public static class CsvRecordWriter extends RecordWriter<IntWritable, Text> {
        private DataOutputStream out;

        public CsvRecordWriter(DataOutputStream stream) {
            out = stream;
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            out.close();
        }

        // Writes the line as string in the csv
        @Override
        public void write(IntWritable arg0, Text arg1) throws IOException, InterruptedException {
            out.writeBytes(arg1.toString());
            out.writeBytes("\r\n");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pivot");
        job.setJarByClass(TransposeMatrix.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(CellMapper.class);
        //job.setCombinerClass(LineReducer.class);
        job.setReducerClass(LineReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(CsvOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
