package wc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SyntheticGraph {
    HashMap<String,String> createGraph(long k_param)
    {
        long pages = k_param * k_param;
        double initPR = (double) 1/pages;
        System.out.println("Init PR: "+initPR);
        System.out.println("Pages: "+pages);
        HashMap<String,String> graphRDD = new HashMap<>();
        for(long pgId = 1; pgId<=pages; pgId++){
            if(pgId % k_param == 0){
                String[] outLinks = new String[1];
                outLinks[0]="0";
                String value = Arrays.toString(outLinks) +","+ initPR;
                graphRDD.put(Long.toString(pgId), value);
            }
            else {
                String[] outLinks = new String[1];
                outLinks[0]= String.valueOf((pgId+1));
                String value = Arrays.toString(outLinks) +","+ initPR;
                graphRDD.put(Long.toString(pgId),value);
            }
        }
        return graphRDD;
    }
     void writeGraph(String filePath, HashMap<String,String> graph){
         BufferedWriter bf = null;
         File file = new File(filePath);
         file.getParentFile().mkdir();
         try {
             bf = new BufferedWriter(new FileWriter(file));
             for(Map.Entry<String, String> entry : graph.entrySet()){
                 bf.write(entry.getKey()+","+entry.getValue());
                 bf.newLine();
             }
             bf.flush();
         }
         catch (IOException e) {
             e.printStackTrace();
         }finally {
             try{
                 assert bf != null;
                 bf.close();
             }catch (Exception e){
                 e.printStackTrace();
             }
         }
     }

    public static void main(String[] args){
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<pagerank-dir> <k-parameter>");
        }
        long k = Integer.parseInt(args[1]);
        String filename = args[0]+"/SyntheticGraph_k_"+k+".csv";
        System.out.println("K parameter: "+ args[1]);
        SyntheticGraph sg = new SyntheticGraph();
        HashMap<String ,String> graph = sg.createGraph(k);
        sg.writeGraph(filename, graph);
    }
}
