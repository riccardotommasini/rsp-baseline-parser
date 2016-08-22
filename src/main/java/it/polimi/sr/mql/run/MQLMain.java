package it.polimi.sr.mql.run;

import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.EPStatementObjectModel;
import it.polimi.sr.mql.MQLQuery;
import it.polimi.sr.mql.events.calculus.MatchClause;
import it.polimi.sr.mql.events.declaration.EventDecl;
import it.polimi.sr.mql.events.declaration.IFDecl;
import it.polimi.sr.mql.parser.MQLParser;
import it.polimi.sr.rsp.streams.Window;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.query.SortCondition;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.lang.SyntaxVarScope;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.ReportingParseRunner;
import org.parboiled.support.ParsingResult;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MQLMain {

    public static void main(String[] args) throws IOException {

        String input = getInput();

        MQLParser parser = Parboiled.createParser(MQLParser.class);

        parser.setResolver(IRIResolver.create());

        ParsingResult<MQLQuery> result = new ReportingParseRunner(parser.Query()).run(input);

        MQLQuery q = result.resultValue;

        print(q);
        System.out.println("Check valid");
        SyntaxVarScope.check(q);
    }

    private static void print(MQLQuery q) throws UnsupportedEncodingException {
        System.out.println("--MQL--");
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
                System.out.println(sc.getExpression().toString() + "  "
                        + ((org.apache.jena.query.Query.ORDER_DESCENDING == sc.direction) ? "DESC" : "ASC"));
            }

        System.out.println("LIMIT " + q.getLimit());
        System.out.println("OFFSET " + q.getOffset());

        VarExprList groupBy = q.getGroupBy();

        System.out.println("GROUP BY");
        List<Var> vars = groupBy.getVars();
        for (Var v : vars) {
            System.out.println("VAR " + v + " EXPR " + groupBy.getExpr(v));
        }

        System.out.println("HAVING");
        List<Expr> havingExprs = q.getHavingExprs();
        for (Expr e : havingExprs) {
            System.out.println("EXPR " + e.toString());
        }
        System.out.println("---");

        if (q.getNamedwindows() != null) {

            for (Node w : q.getNamedwindows().keySet()) {
                System.out.println(q.getNamedwindows().get(w));
            }
        }

        if (q.getWindows() != null) {
            for (Window w : q.getWindows()) {
                System.out.println(w.toString());
            }
        }

        if (q.getWindowGraphElements() != null) {
            for (ElementNamedGraph windowGraphElement : q.getWindowGraphElements()) {
                System.out.println(windowGraphElement.toString());
            }
        }

        if (q.getEventDeclarations() != null) {
            for (String k : q.getEventDeclarations().keySet()) {
                EventDecl x = q.getEventDeclarations().get(k);
                System.out.println(x);
                if (x.getIfdecl() != null) {
                    System.out.println(x.getIfdecl().toSPARQL().toString());
                }
            }
        }

        if (q.getMatchclauses() != null) {
            for (MatchClause matchclause : q.getMatchclauses()) {
                System.out.println(matchclause.toString());
                EPStatementObjectModel epStatementObjectModel = matchclause.toEpl();
                System.out.println(epStatementObjectModel.toEPL());


                for (IFDecl ifDecl : matchclause.getIfDeclarations()) {
                    System.out.println(ifDecl);
                }

                for (Var v : matchclause.getJoinVariables()) {
                    System.out.println(v);
                }

                testEpl(q);

                System.out.println("---");
            }
        }

        System.out.println(q.getHeader());

        System.out.println("--SPARQL--");

        System.out.println(q.toString());

    }

    private static void testEpl(MQLQuery q) throws UnsupportedEncodingException {


        ConfigurationMethodRef ref = new ConfigurationMethodRef();
        Configuration cepConfig = new Configuration();
        cepConfig.getEngineDefaults().getLogging().setEnableExecutionDebug(true);
        cepConfig.getEngineDefaults().getLogging().setEnableTimerDebug(true);


        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("packedId", "string");
        properties.put("ts", "long");

        cepConfig.addEventType("TEvent", properties);
        cepConfig.addEventType("TStream", properties);
        EPServiceProvider cep = EPServiceProviderManager.getProvider("", cepConfig);
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPRuntime cepRT = cep.getEPRuntime();

        Map<String, EventDecl> eventDeclarations = q.getEventDeclarations();


        for (String s : eventDeclarations.keySet()) {
            EventDecl eventDecl = eventDeclarations.get(s);
            cepAdm.createEPL(eventDecl.toEPLSchema());
        }

        EventType[] eventTypes = cepAdm.getConfiguration().getEventTypes();
        for (EventType eventType : eventTypes) {
            String[] propertyNames = eventType.getPropertyNames();
            System.out.println(eventType.getName());
            for (String propertyName : propertyNames) {
                System.out.println(propertyName);
            }
        }

        if (q.getNamedwindows() != null) {

            for (Node w : q.getNamedwindows().keySet()) {
                Window window = q.getNamedwindows().get(w);
                System.out.println(window.toEPLSchema());
                cepAdm.createEPL(window.toEPLSchema());
                EPStatementObjectModel stmt = window.toEPL();
                System.out.println(stmt.toEPL());
                cepAdm.create(stmt);
            }
        }

        if (q.getWindows() != null) {
            for (Window w : q.getWindows()) {
                System.out.println(w.toEPLSchema());
                cepAdm.createEPL(w.toEPLSchema());
                EPStatementObjectModel stmt = w.toEPL();
                System.out.println(stmt.toEPL());
                cepAdm.create(stmt);
            }
        }

        for (MatchClause m : q.getMatchclauses()) {
            EPStatementObjectModel sodaStatement = m.toEpl();
            System.out.println(sodaStatement.toEPL());
            cepAdm.create(sodaStatement);
        }

    }


    public static String getInput() throws IOException {
        File file = new File("/Users/Riccardo/_Projects/Streamreasoning/MQL-Parser/src/main/resources/mqlquery.q");
        return FileUtils.readFileToString(file);
    }

    private static String converVarsToEPLProps(Set<Var> vars) {
        String eplProp = "";
        for (Var var : vars) {
            eplProp += var.getVarName() + " String ,";
        }
        eplProp = eplProp == "" ? "" : eplProp.substring(0, eplProp.length() - 1);
        return eplProp;
    }


}