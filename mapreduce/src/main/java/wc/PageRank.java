package wc;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRank.class);

    //Global factor for converting double to long (Dangling PR)
    private static final long factor = 1000000000;

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        private final Text pgId = new Text();
        private final Text graph = new Text();
        private final Text edge = new Text();
        private final Text pgRnk = new Text();
        //Counter for Dangling pages
        public enum counter1 {DANGLING_PR}
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()){
                String graphLine = tokenizer.nextToken().replaceAll("\\[|\\]", "");
                String []graphParts = graphLine.split(",");
                String pageId = graphParts[0];
                String[] outgoingLink = Arrays.copyOfRange(graphParts,1, graphParts.length-1);
                String pageRank = graphParts[graphParts.length-1];
                pgId.set(pageId);
                graph.set(Arrays.toString(outgoingLink) + "," + pageRank);
                context.write(pgId,graph);

                double newPageRank = Double.parseDouble(pageRank)/outgoingLink.length;
                for (String outEdge : outgoingLink){
                    if(!outEdge.equals("0")){
                        edge.set(outEdge);
                        pgRnk.set(String.valueOf(newPageRank));
                        context.write(edge,pgRnk);
                    }
                    else {
                        long danglingPR = (long) (newPageRank * factor);
                        context.getCounter(counter1.DANGLING_PR).increment(danglingPR);
                    }
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        private double alpha, randJump;
        private final Text adjacencyList = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            long kParameter = Long.parseLong(context.getConfiguration().get("K"));
            long pages = kParameter * kParameter;
            alpha = 0.85;
            randJump = (1-alpha)/pages;
        }

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            double prContribution = 0;
            String[] outlink=null;
            for (final Text value : values){
                String[] adjPrList = value.toString().replaceAll("\\[|\\]", "").split(",");
                if(adjPrList.length > 1){
                    // adjacency list has outlinks and pagerank
                    outlink = Arrays.copyOfRange(adjPrList,0, adjPrList.length-1);
                }
                else{
                    // adjacency list has only pagerank
                    prContribution += Double.parseDouble(adjPrList[adjPrList.length-1]);
                }
            }
            //Calculate new pagerank
            double newPR = randJump + (alpha*prContribution);
            adjacencyList.set(Arrays.toString(outlink)+","+newPR);
            context.write(key,adjacencyList);
        }
    }

    public static class DanglingPRDistroMapper extends Mapper<Object, Text, Text, Text> {
        private final Text pgId = new Text();
        private final Text graph = new Text();
        private static double danglingPR;
        //Counter for Sum of PageRanks
        public enum counter2 {SumPageRanks}
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            double danglingPageRanks = Double.parseDouble(context.getConfiguration().get("DanglingPR"));
            long kParameter = Long.parseLong(context.getConfiguration().get("K"));
            long pages = kParameter * kParameter;
            double alpha = Double.parseDouble(context.getConfiguration().get("Alpha"));
            danglingPR = alpha * danglingPageRanks / pages;
        }

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()){
                String graphLine = tokenizer.nextToken().replaceAll("\\[|\\]", "");
                String []graphParts = graphLine.split(",");
                String pageId = graphParts[0];
                String[] outgoingLink = Arrays.copyOfRange(graphParts,1, graphParts.length-1);
                double pageRank = Double.parseDouble(graphParts[graphParts.length-1]);
                // Adding dangling PageRanks to all real pages
                pageRank = pageRank+danglingPR;
                pgId.set(pageId);
                graph.set(Arrays.toString(outgoingLink) + "," + pageRank);
                context.write(pgId,graph);
                long pr = (long)(pageRank * factor);
                context.getCounter(DanglingPRDistroMapper.counter2.SumPageRanks).increment(pr);
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        conf.set("K", args[2]);
        conf.set("Alpha","0.85");
        int k_Parameter = Integer.parseInt(args[2]);
        int nLine = k_Parameter*k_Parameter / 20;
        int iteration = Integer.parseInt(args[3]);
        int retCode = -1;
        for(int itr=1; itr<=iteration; itr++){
            final Job job1 = Job.getInstance(conf, "PageRank");
            job1.setJarByClass(PageRank.class);
            final Configuration jobConf1 = job1.getConfiguration();
            jobConf1.set("mapreduce.output.textoutputformat.separator", ",");

            job1.setMapperClass(PageRankMapper.class);
            job1.setReducerClass(PageRankReducer.class);
            job1.setInputFormatClass(NLineInputFormat.class);
            if(itr == 1){
                NLineInputFormat.addInputPath(job1, new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_temp_"+itr));
            }
            else{
                NLineInputFormat.addInputPath(job1, new Path(args[0]+"_temp_"+(itr-1)));
                FileOutputFormat.setOutputPath(job1, new Path(args[1]+"_temp_"+itr));
            }
            job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", nLine);
            LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            retCode = job1.waitForCompletion(true) ? 0 : 1;
            if (retCode != 0){
                return retCode;
            }
            else if(retCode == 0){

                long danglingPR = job1.getCounters().findCounter(PageRankMapper.counter1.DANGLING_PR).getValue();
                double danglingPrDistro = (double) danglingPR/factor;
                conf.set("DanglingPR", String.valueOf(danglingPrDistro));
                final Job job2 = Job.getInstance(conf, "DanglingPageRankDistro");
                job2.setJarByClass(PageRank.class);
                final Configuration jobConf2 = job2.getConfiguration();
                jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
                job2.setMapperClass(DanglingPRDistroMapper.class);
                job2.setInputFormatClass(NLineInputFormat.class);

                if(itr == iteration){
                    NLineInputFormat.addInputPath(job2, new Path(args[1]+"_temp_"+itr));
                    FileOutputFormat.setOutputPath(job2, new Path(args[0]+"_final"));
                }
                else {
                    NLineInputFormat.addInputPath(job2, new Path(args[1]+"_temp_"+itr));
                    FileOutputFormat.setOutputPath(job2, new Path(args[0]+"_temp_"+itr));
                }

                job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", nLine);
                LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                retCode = job2.waitForCompletion(true) ? 0 : 1;
                if(retCode ==0){
                    long sumPr = job2.getCounters().findCounter(DanglingPRDistroMapper.counter2.SumPageRanks).getValue();
                    double sumPageRanks = (double) sumPr/factor;
                    System.out.println("K Parameter: "+k_Parameter);
                    System.out.println("Iteration: "+itr);
                    System.out.println("NLine Input: "+nLine);
                    System.out.println("sumPageRanks: "+sumPageRanks);
                }
            }
        }
        return retCode;
    }

    public static void main(final String[] args) {
        if (args.length != 4) {
            throw new Error("Four arguments required:\n<input-dir> <inter-output-dir> <k-parameter> <Iterations>");
        }

        try {
            ToolRunner.run(new PageRank(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}