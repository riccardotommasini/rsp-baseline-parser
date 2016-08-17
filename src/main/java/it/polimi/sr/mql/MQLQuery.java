package it.polimi.sr.mql;

import it.polimi.sr.csparql.Register;
import it.polimi.sr.csparql.Window;
import it.polimi.sr.sparql.Prefix;
import lombok.Data;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.iri.IRI;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryBuildException;
import org.apache.jena.query.QueryException;
import org.apache.jena.riot.checker.CheckerIRI;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.syntax.*;

import java.util.*;

/**
 * Created by Riccardo on 05/08/16.
 */
@Data
public class MQLQuery extends Query {

    private Map<Node, Window> namedwindows;
    private Set<Window> windows;
    private List<ElementNamedGraph> windowGraphElements;
    private Register header;
    private Map<String, EventDecl> eventDeclarations;
    private boolean MQLQyeryStar;
    protected VarExprList MQLprojectVars = new VarExprList();
    private boolean MQLresultVarsSet;

    public MQLQuery(IRIResolver resolver) {
        setBaseURI(resolver);
    }

    public MQLQuery(Prologue prologue) {
        super(prologue);
    }

    public MQLQuery setSelectQuery() {
        setQuerySelectType();
        return this;
    }

    public org.apache.jena.query.Query getQ() {
        return this;
    }

    public MQLQuery setConstructQuery() {
        setQueryConstructType();
        return this;
    }

    public MQLQuery setDescribeQuery() {
        setQueryDescribeType();
        return this;
    }

    public MQLQuery setAskQuery() {
        setQueryAskType();
        return this;
    }

    public MQLQuery setDistinct() {
        setDistinct(true);
        return this;
    }


    public MQLQuery setQueryStar() {
        setQueryResultStar(true);
        return this;
    }

    public MQLQuery addNamedGraphURI(Node_URI match) {
        addNamedGraphURI(match.getURI());
        return this;
    }

    public MQLQuery addGraphURI(Node_URI match) {
        addGraphURI(match.getURI());
        return this;
    }

    public MQLQuery addElement(ElementGroup sub) {
        setQueryPattern(sub);

        TripleCollectorBGP collector = new TripleCollectorBGP();
        List<Element> elements = sub.getElements();
        for (Element element : elements) {
            if (element instanceof ElementPathBlock) {
                ElementPathBlock epb = (ElementPathBlock) element;
                List<TriplePath> list = epb.getPattern().getList();
                for (TriplePath triplePath : list) {
                    collector.addTriple(triplePath.asTriple());
                }
            }
        }
        Template template = new Template(collector.getBGP());
        setCSPARQLConstructTemplate(template);
        return this;
    }

    public TripleCollectorMark insert(TripleCollectorMark acc, Triple t) {
        acc.addTriple(acc.mark(), t);
        return acc;
    }

    public MQLQuery setCSPARLQBaseURI(String match) {
        setBaseURI(match);
        return this;
    }

    public MQLQuery setPrefix(Prefix pop) {
        setPrefix(pop.getPrefix(), pop.getUri());
        return this;
    }

    public Expr allocCSPARQLAggregate(Aggregator custom) {
        return allocAggregate(custom);

    }

    public MQLQuery addOrderBy(Object n) {
        return (n instanceof Node) ? addOrderBy((Node) n) : addOrderBy((Expr) n);
    }

    public MQLQuery addOrderBy(Node n) {
        addOrderBy(n, ORDER_DEFAULT);
        return this;
    }

    public MQLQuery addOrderBy(Expr n) {
        addOrderBy(n, ORDER_DEFAULT);
        return this;
    }

    public MQLQuery addOrderBy(Expr pop, String s) {
        addOrderBy(pop, "DESC".equals(s) ? ORDER_DESCENDING : ORDER_ASCENDING);
        return this;
    }

    public MQLQuery setLimit(String limit) {
        setLimit(Integer.parseInt(limit.trim()));
        return this;
    }

    public MQLQuery setOffset(String offset) {
        setOffset(Integer.parseInt(offset.trim()));
        return this;
    }

    public MQLQuery addCSPARQLGroupBy(Expr pop) {
        addGroupBy((Var) null, pop);
        return this;
    }

    public MQLQuery addCSPARQLGroupBy(Var v, Expr pop) {
        addGroupBy(v, pop);
        return this;
    }

    public MQLQuery addCSPARQLGroupBy(Var v) {
        addGroupBy(v);
        return this;
    }

    public MQLQuery setReduced() {
        setReduced(true);
        return this;
    }

    public MQLQuery addCSPARQLCResultVar(Node pop, Expr pop1) {
        addCSPARQLCResultVar(pop, pop1);
        setQueryResultStar(false);
        return this;
    }

    public MQLQuery addCSPARQLCResultVar(Node pop) {
        addResultVar(pop);
        setQueryResultStar(false);
        return this;
    }

    public MQLQuery addCSPARQLCHavingCondition(Expr pop) {
        addHavingCondition(pop);
        return this;
    }

    public MQLQuery setCSPARQLConstructTemplate(Template template) {
        setConstructTemplate(template);
        return this;
    }

    public MQLQuery addCSPARQLCDescribeNode(Node pop) {
        addDescribeNode(pop);
        setQueryResultStar(false);
        return this;
    }

    public String resolveSilent(String iriStr) {
        IRI iri = resolver.resolveSilent(iriStr);
        CheckerIRI.iriViolations(iri, ErrorHandlerFactory.getDefaultErrorHandler());
        return iri.toString();
    }

    public MQLQuery addNamedWindow(Window pop) {
        if (namedwindows == null)
            namedwindows = new HashMap<Node, Window>();
        if (namedwindows.containsKey(pop.getIri()))
            throw new QueryException("Window already opened on a stream: " + pop.getStream().getIri());
        else
            namedwindows.put(pop.getIri(), pop);
        return this;
    }

    public MQLQuery addWindow(Window pop) {

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

    public MQLQuery addElement(ElementNamedGraph elm) {
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

    public MQLQuery setRegister(Register register) {
        this.header = register;
        return this;
    }

    public MQLQuery addEventDecl(EventDecl ed) {
        if (eventDeclarations == null)
            eventDeclarations = new HashMap<String, EventDecl>();
        eventDeclarations.put(ed.getHead(), ed);
        return this;
    }

    public MQLQuery addEmitVar(Node v) {
        if (!v.isVariable())
            throw new QueryException("Not a variable: " + v);
        this.MQLQyeryStar = false;
        return addEmitVar(v.getName());
    }

    public MQLQuery addEmitVar(String name) {
        Var v = Var.alloc(name);
        this.MQLresultVarsSet = true;
        return _addMQLVar(MQLprojectVars, v);
    }

    private MQLQuery _addMQLVar(VarExprList varlist, Var v) {
        if (varlist == null)
            varlist = new VarExprList();

        if (varlist.contains(v)) {
            Expr expr = varlist.getExpr(v);
            if (expr != null)
                throw new QueryBuildException("Duplicate variable (had an expression) in result projection '" + v + "'");
        }
        varlist.add(v);
        return this;
    }


    public MQLQuery setMQLQueryStar() {
        this.MQLQyeryStar = true;
        return this;
    }


}