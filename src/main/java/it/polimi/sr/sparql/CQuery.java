package it.polimi.sr.sparql;

import it.polimi.sr.csparql.Register;
import it.polimi.sr.csparql.Window;
import it.polimi.sr.mql.EventDecl;
import lombok.Data;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.IRI;
import org.apache.jena.query.QueryException;
import org.apache.jena.riot.checker.CheckerIRI;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementNamedGraph;
import org.apache.jena.sparql.syntax.Template;
import org.apache.jena.sparql.syntax.TripleCollectorMark;

import java.util.*;

/**
 * Created by Riccardo on 05/08/16.
 */
@Data
public class CQuery extends org.apache.jena.query.Query {

    private Map<Node, Window> namedwindows;
    private Set<Window> windows;
    private List<ElementNamedGraph> windowGraphElements;
    private Register header;
    private Map<String, EventDecl> eventDeclarations;

    public CQuery(IRIResolver resolver) {
        setBaseURI(resolver);
    }

    public CQuery(Prologue prologue) {
        super(prologue);
    }

    public CQuery setSelectQuery() {
        setQuerySelectType();
        return this;
    }

    public org.apache.jena.query.Query getQ() {
        return this;
    }

    public CQuery setConstructQuery() {
        setQueryConstructType();
        return this;
    }

    public CQuery setDescribeQuery() {
        setQueryDescribeType();
        return this;
    }

    public CQuery setAskQuery() {
        setQueryAskType();
        return this;
    }

    public CQuery setDistinct() {
        setDistinct(true);
        return this;
    }


    public CQuery setQueryStar() {
        setQueryResultStar(true);
        return this;
    }

    public CQuery addNamedGraphURI(Node_URI match) {
        addNamedGraphURI(match.getURI());
        return this;
    }

    public CQuery addGraphURI(Node_URI match) {
        addGraphURI(match.getURI());
        return this;
    }

    public CQuery addElement(Element sub) {
        setQueryPattern(sub);
        return this;
    }

    public TripleCollectorMark insert(TripleCollectorMark acc, Triple t) {
        acc.addTriple(acc.mark(), t);
        return acc;
    }

    public CQuery setCSPARLQBaseURI(String match) {
        setBaseURI(match);
        return this;
    }

    public CQuery setPrefix(Prefix pop) {
        setPrefix(pop.getPrefix(), pop.getUri());
        return this;
    }

    public Expr allocCSPARQLAggregate(Aggregator custom) {
        return allocAggregate(custom);

    }

    public CQuery addOrderBy(Object n) {
        return (n instanceof Node) ? addOrderBy((Node) n) : addOrderBy((Expr) n);
    }

    public CQuery addOrderBy(Node n) {
        addOrderBy(n, ORDER_DEFAULT);
        return this;
    }

    public CQuery addOrderBy(Expr n) {
        addOrderBy(n, ORDER_DEFAULT);
        return this;
    }

    public CQuery addOrderBy(Expr pop, String s) {
        addOrderBy(pop, "DESC".equals(s) ? ORDER_DESCENDING : ORDER_ASCENDING);
        return this;
    }

    public CQuery setLimit(String limit) {
        setLimit(Integer.parseInt(limit.trim()));
        return this;
    }

    public CQuery setOffset(String offset) {
        setOffset(Integer.parseInt(offset.trim()));
        return this;
    }

    public CQuery addCSPARQLGroupBy(Expr pop) {
        addGroupBy((Var) null, pop);
        return this;
    }

    public CQuery addCSPARQLGroupBy(Var v, Expr pop) {
        addGroupBy(v, pop);
        return this;
    }

    public CQuery addCSPARQLGroupBy(Var v) {
        addGroupBy(v);
        return this;
    }

    public CQuery setReduced() {
        setReduced(true);
        return this;
    }

    public CQuery addCSPARQLCResultVar(Node pop, Expr pop1) {
        addCSPARQLCResultVar(pop, pop1);
        setQueryResultStar(false);
        return this;
    }

    public CQuery addCSPARQLCResultVar(Node pop) {
        addResultVar(pop);
        setQueryResultStar(false);
        return this;
    }

    public CQuery addCSPARQLCHavingCondition(Expr pop) {
        addHavingCondition(pop);
        return this;
    }

    public CQuery setCSPARQLConstructTemplate(Template template) {
        setConstructTemplate(template);
        return this;
    }

    public CQuery addCSPARQLCDescribeNode(Node pop) {
        addDescribeNode(pop);
        setQueryResultStar(false);
        return this;
    }

    public String resolveSilent(String iriStr) {
        IRI iri = resolver.resolveSilent(iriStr);
        CheckerIRI.iriViolations(iri, ErrorHandlerFactory.getDefaultErrorHandler());
        return iri.toString();
    }

    public CQuery addNamedWindow(Window pop) {
        if (namedwindows == null)
            namedwindows = new HashMap<Node, Window>();
        if (namedwindows.containsKey(pop.getIri()))
            throw new QueryException("Window already opened on a stream: " + pop.getStream().getIri());
        else
            namedwindows.put(pop.getIri(), pop);
        return this;
    }

    public CQuery addWindow(Window pop) {

        if (pop.getIri() != null) {
            return addNamedWindow(pop);
        }

        if (windows == null)
            windows = new HashSet<Window>();
        if (windows.contains(pop))
            throw new QueryException("Window already opened on a stream: " + pop.getStream().getIri());
        else
            windows.add(pop);
        return this;
    }

    public CQuery addElement(ElementNamedGraph elm) {
        if (windowGraphElements == null) {
            windowGraphElements = new ArrayList<ElementNamedGraph>();
        }
        windowGraphElements.add(elm);
        return this;
    }


    @Override
    public String toString() {
        return super.toString();
    }

    public CQuery setRegister(Register register) {
        this.header = register;
        return this;
    }

    public CQuery addEventDecl(EventDecl ed) {
        if (eventDeclarations == null)
            eventDeclarations = new HashMap<String, EventDecl>();
        eventDeclarations.put(ed.getHead(), ed);
        return this;
    }
}