package ru.itmo;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import ru.itmo.map.SaleInfo;
import ru.itmo.map.SaleMapper;
import ru.itmo.map.SaleReducer;
import ru.itmo.sort.ShuffleData;
import ru.itmo.sort.ShuffleMapper;
import ru.itmo.sort.ShuffleReducer;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Необходимо 4 аргумента");
            System.exit(-1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        int reducersCount = Integer.parseInt(args[2]);
        int datablockSizeMb = Integer.parseInt(args[3]) * ((int) Math.pow(2, 10));
        String intermediateResultDir = outputDir + "-intermediate";

        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path intermediateOutput = new Path(intermediateResultDir);
        Path finalOutput = new Path(outputDir);

        if (fs.exists(intermediateOutput)) {
            log.info("Удаление существующей промежуточной директории: {}", intermediateResultDir);
            fs.delete(intermediateOutput, true);
        }

        if (fs.exists(finalOutput)) {
            log.info("Удаление существующей итоговой директории: {}", outputDir);
            fs.delete(finalOutput, true);
        }

        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(datablockSizeMb));

        Job salesAnalysisJob = Job.getInstance(conf, "map sales");
        salesAnalysisJob.setNumReduceTasks(reducersCount);
        salesAnalysisJob.setJarByClass(Main.class);
        salesAnalysisJob.setMapperClass(SaleMapper.class);
        salesAnalysisJob.setReducerClass(SaleReducer.class);
        salesAnalysisJob.setMapOutputKeyClass(Text.class);
        salesAnalysisJob.setMapOutputValueClass(SaleInfo.class);
        salesAnalysisJob.setOutputKeyClass(Text.class);
        salesAnalysisJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(salesAnalysisJob, new Path(inputDir));
        intermediateOutput = new Path(intermediateResultDir);
        FileOutputFormat.setOutputPath(salesAnalysisJob, intermediateOutput);

        boolean success = salesAnalysisJob.waitForCompletion(false);

        if (!success) {
            System.exit(1);
        }

        Job sortByValueJob = Job.getInstance(conf, "sorting by prize");
        sortByValueJob.setJarByClass(Main.class);
        sortByValueJob.setMapperClass(ShuffleMapper.class);
        sortByValueJob.setReducerClass(ShuffleReducer.class);

        sortByValueJob.setMapOutputKeyClass(DoubleWritable.class);
        sortByValueJob.setMapOutputValueClass(ShuffleData.class);

        sortByValueJob.setOutputKeyClass(ShuffleData.class);
        sortByValueJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortByValueJob, intermediateOutput);
        FileOutputFormat.setOutputPath(sortByValueJob, new Path(outputDir));

        long endTime = System.currentTimeMillis();
        log.info("Jobs completed in {} milliseconds", (endTime - startTime));

        System.exit(sortByValueJob.waitForCompletion(false) ? 0 : 1);
    }
}
