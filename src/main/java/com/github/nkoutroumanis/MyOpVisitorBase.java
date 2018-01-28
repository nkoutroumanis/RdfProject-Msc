/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.nkoutroumanis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;

/**
 *
 * @author nicholaskoutroumanis
 */
public class MyOpVisitorBase extends OpVisitorBase {

    private static String triplets;
    private static Map<String, Integer> dictionary;
    private static MyOpVisitorBase m = new MyOpVisitorBase();

    public void myOpVisitorWalker(Op op) {
        OpWalker.walk(op, this);
    }

    @Override
    public void visit(final OpBGP opBGP) {
        final List<Triple> triples = opBGP.getPattern().getList();
        int i = 0;
        for (final Triple triple : triples) {
            triplets = triple.toString();
            System.out.println("Triple: " + triple.toString());
        }
    }

    public static void getTriples(String q) {
        Query query = QueryFactory.create(q);
        Op op = Algebra.compile(query);
        m.myOpVisitorWalker(op);
    }

    public static String sparqlToEncodedSql(String sparql) throws IOException {
        getTriples(sparql);
        //create the HashMap if dictionary is null
        if (dictionary == null) {
            dictionary = new HashMap<>();
            Files.lines(Paths.get(PartitionQueryingExample.dictionaryPath)).forEach(new Consumer<String>() {
                @Override
                public void accept(String s) {
                    dictionary.put(s.split("	", 2)[1], Integer.parseInt(s.split("	", 2)[0]));
                }
            }
            );
        }

        //get Subject, Predice, Object to a three-element array
        String[] tokens = triplets.split("\\s+");

        int[] encodedtokens = new int[3];
        //encode the triplets from SPARQL
        //Subject
        encodedtokens[0] = (tokens[0].substring(0, 1).equals("?")) ? 0 : dictionary.get(tokens[0].substring(1, tokens[0].length() - 1));
        //Predicate
        encodedtokens[1] = (tokens[1].substring(1, 2).equals("?")) ? 0 : (tokens[1].substring(1, 2).equals(":")) ? dictionary.get(tokens[1].substring(1)) : dictionary.get(tokens[1].substring(tokens[1].lastIndexOf("/") + 1));
        //Object
        encodedtokens[2] = (tokens[2].substring(0, 1).equals("?")) ? 0 : dictionary.get(tokens[2].substring(1, tokens[2].length() - 1));

        String table = null;
        //check if the subject is positive or negative
        if (encodedtokens[0] > 0) {
            table = "Positive";
        } else if (encodedtokens[0] < 0) {
            table = "Negative";
        }

        String formQuery = null;
        //if table is defined through Subject
        if (table != null) {
            if (encodedtokens[1] == 0 && encodedtokens[2] == 0) {
                formQuery = "SELECT * FROM" + " " + table + " WHERE Subject=" + encodedtokens[0];

            } else if (encodedtokens[1] != 0 && encodedtokens[2] == 0) {
                formQuery = "SELECT * FROM" + " " + table + " WHERE Subject=" + encodedtokens[0] + " AND Predicate=" + encodedtokens[1];

            } else if (encodedtokens[1] == 0 && encodedtokens[2] != 0) {
                formQuery = "SELECT * FROM" + " " + table + " WHERE Subject=" + encodedtokens[0] + " AND Object=" + encodedtokens[2];

            } else if (encodedtokens[1] != 0 && encodedtokens[2] != 0) {
                formQuery = "SELECT * FROM" + " " + table + " WHERE Subject=" + encodedtokens[0] + " AND Predicate=" + encodedtokens[1] + " AND Object=" + encodedtokens[2];

            }
        } else { //if table is not defined through Subject
            if (encodedtokens[1] == 0 && encodedtokens[2] == 0) {
                formQuery = "SELECT * FROM Positive"
                        + " UNION ALL "
                        + "SELECT * FROM Negative";

            } else if (encodedtokens[1] != 0 && encodedtokens[2] == 0) {
                formQuery = "SELECT * FROM Positive WHERE Predicate=" + encodedtokens[1]
                        + " UNION ALL "
                        + "SELECT * FROM Negative WHERE Predicate=" + encodedtokens[1];

            } else if (encodedtokens[1] == 0 && encodedtokens[2] != 0) {
                formQuery = "SELECT * FROM Positive WHERE Object=" + encodedtokens[2]
                        + " UNION ALL "
                        + "SELECT * FROM Negative WHERE Object=" + encodedtokens[2];

            } else if (encodedtokens[1] != 0 && encodedtokens[2] != 0) {
                formQuery = "SELECT * FROM Positive WHERE Predicate=" + encodedtokens[1] + " AND Object=" + encodedtokens[2]
                        + " UNION ALL "
                        + "SELECT * FROM Negative WHERE Predicate=" + encodedtokens[1] + " AND Object=" + encodedtokens[2];
            }
        }

        return formQuery;

    }
    public static void main(String args[]) throws IOException
    {
        System.out.println(sparqlToEncodedSql("SELECT * WHERE {':node_376609000_1451606409000_-9.15947_38.70289' <a> ?x . ?x <a> ':Node'}"));
    }
}
