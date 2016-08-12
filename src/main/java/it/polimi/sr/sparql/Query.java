package it.polimi.sr.sparql;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.IRI;
import org.apache.jena.riot.checker.CheckerIRI;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.syntax.TripleCollectorMark;

/**
 * Created by Riccardo on 05/08/16.
 */
public class Query extends org.apache.jena.query.Query {

    private int orderDescending = org.apache.jena.query.Query.ORDER_DESCENDING;
    private int orderAscending = org.apache.jena.query.Query.ORDER_ASCENDING;

    public Query(IRIResolver resolver) {
        setBaseURI(resolver);
    }

    public Query(Prologue prologue) {
        super(prologue);
    }

    public Query setSelectQuery() {
        setQuerySelectType();
        return this;
    }

    public org.apache.jena.query.Query getQ() {
        return this;
    }

    public Query setConstructQuery() {
        setQueryConstructType();
        return this;
    }

    public Query setDescribeQuery() {
        setQueryDescribeType();
        return this;
    }

    public Query setAskQuery() {
        setQueryAskType();
        return this;
    }

    public Query setDistinct() {
        setDistinct(true);
        return this;
    }


    @Override
    public String toString() {
        return toString();
    }

    public Query setQueryStar() {
        setQueryResultStar(true);
        return this;
    }

    public Query addNamedWindowURI(Node_URI stream) {
        return this;
    }

    public Query addNamedGraphURI(Node_URI match) {
        addNamedGraphURI(match.getURI());
        return this;
    }

    public Query addGraphURI(Node_URI match) {
        addGraphURI(match.getURI());
        return this;
    }

    public Query addElement(Element sub) {
        setQueryPattern(sub);
        return this;
    }

    public TripleCollectorMark insert(TripleCollectorMark acc, Triple t) {
        acc.addTriple(acc.mark(), t);
        return acc;
    }

    public Query setCSPARLQBaseURI(String match) {
        setBaseURI(match);
        return this;
    }

    public Query setPrefix(Prefix pop) {
        setPrefix(pop.getPrefix(), pop.getUri());
        return this;
    }

    public Expr allocCSPARQLAggregate(Aggregator custom) {
        return allocAggregate(custom);

    }

    public Query addOrderBy(Object n) {
        return (n instanceof Node) ? addOrderBy((Node) n) : addOrderBy((Expr) n);
    }

    public Query addOrderBy(Node n) {
        addOrderBy(n, org.apache.jena.query.Query.ORDER_DEFAULT);
        return this;
    }

    public Query addOrderBy(Expr n) {
        addOrderBy(n, org.apache.jena.query.Query.ORDER_DEFAULT);
        return this;
    }

    public Query addOrderBy(Expr pop, String s) {
        addOrderBy(pop, "DESC".equals(s) ? orderDescending : orderAscending);
        return this;
    }

    public Query setLimit(String limit) {
        setLimit(Integer.parseInt(limit.trim()));
        return this;
    }

    public Query setOffset(String offset) {
        setOffset(Integer.parseInt(offset.trim()));
        return this;
    }

    public Query addCSPARQLGroupBy(Expr pop) {
        addGroupBy((Var) null, pop);
        return this;
    }

    public Query addCSPARQLGroupBy(Var v, Expr pop) {
        addGroupBy(v, pop);
        return this;
    }

    public Query addCSPARQLGroupBy(Var v) {
        addGroupBy(v);
        return this;
    }

    public Query setReduced() {
        setReduced(true);
        return this;
    }

    public Query addCSPARQLCResultVar(Node pop, Expr pop1) {
        addCSPARQLCResultVar(pop, pop1);
        setQueryResultStar(false);
        return this;
    }

    public Query addCSPARQLCResultVar(Node pop) {
        addResultVar(pop);
        setQueryResultStar(false);
        return this;
    }

    public Query addCSPARQLCHavingCondition(Expr pop) {
        addHavingCondition(pop);
        return this;
    }

    public Query setCSPARQLConstructTemplate(Template template) {
        setConstructTemplate(template);
        return this;
    }

    public Query addCSPARQLCDescribeNode(Node pop) {
        addDescribeNode(pop);
        setQueryResultStar(false);
        return this;
    }

    public String resolveSilent(String iriStr) {
        IRI iri = resolver.resolveSilent(iriStr);
        CheckerIRI.iriViolations(iri, ErrorHandlerFactory.getDefaultErrorHandler());
        return iri.toString();
    }
}