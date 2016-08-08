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
import org.apache.jena.query.SortCondition;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.lang.ParserSPARQL11;
import org.apache.jena.sparql.lang.SPARQLParser;
import org.apache.jena.sparql.lang.SyntaxVarScope;
import org.apache.jena.sparql.syntax.Element;
import org.parboiled.Node;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {

        String input = getInput();

        //jena
        org.apache.jena.query.Query query = new org.apache.jena.query.Query();

        SPARQLParser jenaParser = new ParserSPARQL11();
        IRIResolver resolver = IRIResolver.create();
        query.setResolver(resolver);
        org.apache.jena.query.Query parsed = jenaParser.parse(query, input);
        print(parsed);

        SparqlParser parser = Parboiled.createParser(SparqlParser.class);
        ParsingResult<Query> result = new ReportingParseRunner(parser.Query()).run(input);
        Node<Query> n = result.parseTreeRoot.getChildren().get(0);
        org.apache.jena.query.Query q = n.getValue().getQ();

        print(q);
        System.out.println("Check valid");
        SyntaxVarScope.check(q);


    }

    private static void print(org.apache.jena.query.Query q) {
        System.out.println("---");
        System.out.println(q.getGraphURIs());
        System.out.println(q.getQueryType());
        System.out.println(q.getNamedGraphURIs());

        VarExprList project = q.getProject();

        if (q.isSelectType()) {

            for (Var v : project.getVars()) {
                System.out.println("Project Var " + v.toString() + " Expr " + project.getExpr(v));
            }
            for (String v : q.getResultVars()) {
                System.out.println("Result Var " + v);
            }
        } else if (q.isConstructType()) {
            Map<org.apache.jena.graph.Node, BasicPattern> graphPattern = q.getConstructTemplate().getGraphPattern();
            for (org.apache.jena.graph.Node b : graphPattern.keySet()) {
                System.out.println("Node " + b + " Pattern " + graphPattern.get(b));
            }
        }

        Element queryPattern = q.getQueryPattern();
        System.out.println("queryPattern " + queryPattern);

        System.out.println("PREFIXES");

        Map<String, String> nsPrefixMap = q.getPrologue().getPrefixMapping().getNsPrefixMap();
        for (String prefix : nsPrefixMap.keySet()) {
            String uri = nsPrefixMap.get(prefix);
            System.out.println(prefix + ":" + uri);
        }

        List<SortCondition> orderBy = q.getOrderBy();

        if (orderBy != null && !orderBy.isEmpty())
            for (SortCondition sc : orderBy) {
                System.out.println(sc.getExpression().toString() + "  " +
                        ((org.apache.jena.query.Query.ORDER_DESCENDING == sc.direction) ? "DESC" : "ASC"));
            }

        System.out.println("LIMIT " + q.getLimit());
        System.out.println("OFFSET " + q.getOffset());

        VarExprList groupBy = q.getGroupBy();

        System.out.println("GROUP BY");
        List<Var> vars = groupBy.getVars();
        for (Var v : vars) {
            System.out.println("VAR " + v + " EXPR " +
                    groupBy.getExpr(v));
        }

        System.out.println("HAVING");
        List<Expr> havingExprs = q.getHavingExprs();
        for (Expr e : havingExprs) {
            System.out.println("EXPR " + e.toString());
        }
        System.out.println("---");


    }

    public static String getInput() throws IOException {
        File file = new File("/Users/Riccardo/_Projects/Streamreasoning/c-sparql_parser/src/main/resources/query.q");
        return FileUtils.readFileToString(file);
    }
}