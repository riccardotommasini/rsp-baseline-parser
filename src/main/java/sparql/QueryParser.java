package sparql;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.aggregate.Args;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.sparql.syntax.*;
import org.apache.jena.sparql.util.ExprUtils;
import org.apache.jena.vocabulary.RDF;
import org.parboiled.BaseParser;

/**
 * Created by Riccardo on 09/08/16.
 */
public class QueryParser extends BaseParser<Object> {

    // NodeConst
    protected final Node XSD_TRUE = NodeConst.nodeTrue;
    protected final Node XSD_FALSE = NodeConst.nodeFalse;

    protected final Node nRDFtype = NodeConst.nodeRDFType;

    protected final Node nRDFnil = NodeConst.nodeNil;
    protected final Node nRDFfirst = NodeConst.nodeFirst;
    protected final Node nRDFrest = NodeConst.nodeRest;

    protected final Node nRDFsubject = RDF.Nodes.subject;
    protected final Node nRDFpredicate = RDF.Nodes.predicate;
    protected final Node nRDFobject = RDF.Nodes.object;

    private IRIResolver resolver;


    public Query getQuery(int i) {
        if (i == -1) {
            int size = getContext().getValueStack().size();
            i = size > 0 ? size - 1 : 0;
        }
        return (Query) peek(i);
    }

    public Query popQuery(int i) {
        if (i == -1) {
            int size = getContext().getValueStack().size();
            i = size > 0 ? size - 1 : 0;
        }
        return (Query) pop(i);
    }

    public boolean pushQuery(Query q) {
        return push(0, q);
    }


    public Element popElement() {
        return ((Element) pop());
    }


    public boolean addElementToQuery() {
        getQuery(1).addElement(popElement());
        return true;
    }

    public boolean addTemplateToQuery() {
        getQuery(1).setConstructTemplate(new Template((((TripleCollectorBGP) pop()).getBGP())));
        return true;

    }

    public boolean addTemplateAndPatternToQuery() {
        ((ElementGroup) peek(1)).addElement(new ElementPathBlock(((TripleCollectorBGP) peek()).getBGP()));
        getQuery(2).setConstructTemplate(new Template((((TripleCollectorBGP) pop()).getBGP())));
        return true;

    }

    public boolean addSubElement() {
        return addSubElement(1);
    }

    public boolean addSubElement(int i) {
        ((ElementGroup) peek(i)).addElement(popElement());
        return true;
    }

    public boolean addFilterElement() {
        return push(new ElementFilter((Expr) pop()));
    }

    public boolean addOptionalElement() {
        return push(new ElementOptional(popElement()));
    }

    public boolean createUnionElement() {
        return push(new ElementUnion(popElement()));
    }

    public boolean addUnionElement() {
        ((ElementUnion) peek(1)).addElement(popElement());
        return true;
    }

    public boolean addTripleToBloc(TripleCollector peek) {
        peek.addTriple(new Triple((Node) peek(2), (Node) peek(1), (Node) pop()));
        return true;
    }

    public boolean addNamedGraphElement() {
        return push(new ElementNamedGraph((Node) pop(), popElement()));
    }

    public boolean addFunctionCall() {
        return push(((Function) pop()).build());
    }

    public boolean addArg() {
        ((Args) peek(1)).add((Expr) pop());
        return true;
    }

    public boolean allocVariable(String s) {
        return push(Var.alloc(s.substring(1)));
    }

    public boolean asExpr() {
        return push(ExprUtils.nodeToExpr((Node) pop()));
    }

    public boolean addExprToExprList() {
        ((ExprList) peek(1)).add((Expr) pop());
        return true;
    }

    void debug(String calls) {
        System.out.println(calls);
    }

    public String trimMatch() {
        return match().trim();
    }

    public String URIMatch() {
        return getQuery(-1).resolveSilent(trimMatch().replace(">", "").replace("<", ""));
    }

    public boolean resolvePNAME(String match) {
        //TODO I think this is correct beacause subqueries refer to the same prologue
        String uri = getQuery(-1).getQ().getPrologue().expandPrefixedName(match);
        return push(NodeFactory.createURI(uri));
    }

    public RDFDatatype getSafeTypeByName(String uri) {
        debug(uri);
        RDFDatatype safeTypeByName = TypeMapper.getInstance().getSafeTypeByName(uri);
        return safeTypeByName;
    }

    public void setResolver(IRIResolver resolver) {
        this.resolver = resolver;
    }

    public IRIResolver getResolver() {
        return resolver;
    }
}
