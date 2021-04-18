package org.dtim.odin.storage.tests.resultsParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class measureMedians {

    public static void main(String[] args) throws Exception {
        System.out.println("|F|;|E_Q|;|W|;|E_W|;Frac_Q;Frac_W;SIZE_OF_INTERMEDIATE_RESULTS;|UCQ|;R (ms);Timeout (ms);MiniCon (ms);Graal (ms)");
        //System.out.println("|F|;|E_Q|;|W|;|E_W|;Frac_Q;Frac_W;R (ms);Timeout (ms);MiniCon (ms);Graal (ms)");
        //System.out.println("|F|;|E_Q|;|W|;|E_W|;Frac_Q;Frac_W;R (ms);Timeout (ms);Graal (ms)");

        String path = "/home/snadal/Desktop/mar2021_experiments/completequeries_raw_2.csv";

        List<String> rawLines = Lists.newArrayList();
        Files.readAllLines(new File(path).toPath()).forEach(rawLines::add);

        List<String> lines = Lists.newArrayList();
        for (int i = 0 ; i < rawLines.size(); i+=2) {
            lines.add(rawLines.get(i) + ";" + rawLines.get(i+1));
        }

        Map<String, List<String>> allData = Maps.newHashMap();
        lines.forEach(line -> {
            String[] split = line.split(";");

            String dimensions = split[0]+";"+split[1]+";"+split[2]+";"+split[3]+";"+split[4]+";"+split[5];
            //ignore 6 and 7
            String values = split[8]+";"+split[9]+";"+split[10];//+";"+split[11];
            allData.putIfAbsent(dimensions, Lists.newArrayList());

            List<String> oldList = allData.get(dimensions);
            oldList.add(values);

            allData.put(dimensions,oldList);
        });

        allData.forEach((dimension,values) -> {
            // AVG
            /*
            int X = 0, Y = 0, Z = 0;
            int N = 0;
            for(String v : values) {
                X += Integer.parseInt(v.split(";")[0]);
                Y += Integer.parseInt(v.split(";")[1]);
                Z += Integer.parseInt(v.split(";")[2]);
                N++;
            };
            System.out.print(dimension+";");
            System.out.println((double)(X)/(double)(N)+";"+(double)(Y)/(double)(N)+";"+(double)(Z)/(double)(N));
            */

            //MEDIAN
            System.out.print(dimension);
            for (int i = 0; i < values.get(0).split(";").length; ++i) {
                List<Double> all = Lists.newArrayList();
                for (int j = 0; j < values.size(); ++j) {
                    all.add(Double.parseDouble(values.get(j).split(";")[i]));
                }
                Collections.sort(all);
                System.out.print(";"+all.get(all.size()/2).intValue());
            }
            System.out.println(";0");

            /*
            values.sort((s, t1) -> {
                int a = Integer.parseInt(s.split(";")[2]);
                int b = Integer.parseInt(t1.split(";")[2]);
                return Integer.compare(a,b);
            });

            System.out.print(dimension+";");
            if (values.size() == 1 || values.size() == 2) System.out.println(values.get(0));
            else System.out.println(values.get(1));*/
        });

    }

}
