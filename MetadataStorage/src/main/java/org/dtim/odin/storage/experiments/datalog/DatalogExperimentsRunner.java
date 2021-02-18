package org.dtim.odin.storage.experiments.datalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import upc.AlgorithmExecutor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatalogExperimentsRunner {

    public static Set<String> runMiniCon(Set<DatalogQuery> queries) throws IOException {
        Set<DatalogQuery> query = queries.stream().filter(t->t.toString().contains("q")).collect(Collectors.toSet());
        Set<DatalogQuery> views = queries.stream().filter(t->t.toString().contains("w")).collect(Collectors.toSet());

        File viewsFile = new File("/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.views");
        File queryFile = new File("/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/miniconPython/minicon.query");

        java.nio.file.Files.write(viewsFile.toPath(),"".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        java.nio.file.Files.write(queryFile.toPath(),"".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

        query.forEach(q -> {
            try {
                Files.append(q.toString(),queryFile, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        views.forEach(q -> {
            try {
                Files.append(q.toString(),viewsFile, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return Sets.newHashSet();

        /*Set<String> rewritings = Sets.newHashSet();
        AlgorithmExecutor.minicon(queryFile.getAbsolutePath(),viewsFile.getAbsolutePath()).forEach(r -> {
            rewritings.add(r.toString());
        });
        return rewritings;*/
    }
/**
    public static Set<String> runCoreCover(Set<DatalogQuery> queries) throws IOException {
        Set<DatalogQuery> query = queries.stream().filter(t->t.toString().contains("q")).collect(Collectors.toSet());
        Set<DatalogQuery> views = queries.stream().filter(t->t.toString().contains("w")).collect(Collectors.toSet());

        File queryFile = File.createTempFile(UUID.randomUUID().toString(),".query");
        File viewsFile = File.createTempFile(UUID.randomUUID().toString(),".views");

        query.forEach(q -> {
            try {
                Files.append(q.toString(),queryFile, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        views.forEach(q -> {
            try {
                Files.append(q.toString(),viewsFile, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Set<String> rewritings = Sets.newHashSet();
        AlgorithmExecutor.corecover(queryFile.getAbsolutePath(),viewsFile.getAbsolutePath()).forEach(r -> {
            rewritings.add(r.toString());
        });
        return rewritings;
    }
**/
    public static Set<String> runGraal(Set<DatalogQuery> queries) throws IOException {
        Set<DatalogQuery> query = queries.stream().filter(t->t.toString().contains("q")).collect(Collectors.toSet());
        Set<DatalogQuery> views = queries.stream().filter(t->t.toString().contains("w")).collect(Collectors.toSet());

        File viewsFile = new File("/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.views");
        File queryFile = new File("/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/graal.query");

        java.nio.file.Files.write(viewsFile.toPath(),"".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
        java.nio.file.Files.write(queryFile.toPath(),"".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

        List<DatalogQuery> listOfViews = Lists.newArrayList(views);

        // Graal query format: ?(A1,A2,A3,B1,B2) :- a(A1,A2,A3,B1),b(B1,B2,C1,C2).
        String q = Iterators.getLast(query.iterator()).toString().replace("q","?")+".";
        Files.append(q, queryFile, Charset.defaultCharset());

        // Graal view format: [R0] a(A1,A2,A3,B1),b(B1,B2,C1,C2) :- w1(A1,A2,B1).
        for (int i = 0; i < listOfViews.size(); i++) {
            DatalogQuery v = listOfViews.get(i);
            try {
                Files.append(v.toGraalString(i),viewsFile, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
/*
        String process = "/usr/bin/java -jar " +
                "/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/pure-rewriter-1.1.0.jar " +
                "rewrite "+viewsFile.getAbsolutePath()+" -q \""+q+"\"";
        String[] proc = new String[]{"sh","/usr/bin/java -jar",
                "/home/snadal/UPC/Sergi/Papers/2020/GraphQueryRewriting/evaluation/graal/pure-rewriter-1.1.0.jar",
                "rewrite",
                viewsFile.getAbsolutePath(),
                "-q",
                "\""+q+"\""};

        try {
            Process p = Runtime.getRuntime().exec(proc);
            p.waitFor();
            System.out.println(IOUtils.toString(p.getInputStream(),Charset.defaultCharset()));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
*/
        return Sets.newHashSet();
    }

}
