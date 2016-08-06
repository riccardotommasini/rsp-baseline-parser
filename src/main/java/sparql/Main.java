package sparql;/*
 * Copyright (C) 2009-2011 Mathias Doenitz
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.syntax.Element;
import org.parboiled.Node;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {
        SparqlParser parser = Parboiled.createParser(SparqlParser.class);

        String input = getInput();

        ParsingResult<Query> result = new ReportingParseRunner(parser.Query()).run(input);
        Node<Query> n = result.parseTreeRoot.getChildren().get(0);
        org.apache.jena.query.Query q = n.getValue().getQ();

        print(q);

        //jena
        print(QueryFactory.create(input));


    }

    private static void print(org.apache.jena.query.Query q) {
        System.out.println("---");
        System.out.println(q.getGraphURIs());
        System.out.println(q.getQueryType());
        System.out.println(q.getNamedGraphURIs());
        for (Var v : q.getProjectVars()) {
            System.out.println("PV " + v.toString());
        }
        for (String v : q.getResultVars()) {
            System.out.println("RV " + v);
        }

        Element queryPattern = q.getQueryPattern();
        System.out.println("queryPattern " + queryPattern);

        System.out.println("PREFIXES");

        Map<String, String> nsPrefixMap = q.getPrologue().getPrefixMapping().getNsPrefixMap();
        for (String prefix : nsPrefixMap.keySet()) {
            String uri = nsPrefixMap.get(prefix);
            System.out.println(prefix + ":" + uri);
        }
        System.out.println("---");

    }

    public static String getInput() throws IOException {
        File file = new File("/Users/Riccardo/_Projects/Streamreasoning/c-sparql_parser/src/main/resources/query.q");
        return FileUtils.readFileToString(file);
    }
}