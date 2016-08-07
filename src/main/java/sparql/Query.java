package sparql;

import lombok.Data;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.TripleCollectorMark;

/**
 * Created by Riccardo on 05/08/16.
 */
@Data
public class Query {

    private org.apache.jena.query.Query q;

    private int orderDescending = org.apache.jena.query.Query.ORDER_DESCENDING;
    private int orderAscending = org.apache.jena.query.Query.ORDER_ASCENDING;

    public Query() {
        this.q = new org.apache.jena.query.Query();
    }

    public Query setSelectQuery() {
        q.setQuerySelectType();
        return this;
    }


    public Query setConstructQuery() {
        q.setQueryConstructType();
        return this;
    }

    public Query setDescribeQuery() {
        q.setQueryDescribeType();
        return this;
    }

    public Query setAskQuery() {
        q.setQueryAskType();
        return this;
    }

    public Query setDistinct(String match) {
        q.setDistinct("DISTINCT".equals(match));
        return this;
    }


    @Override
    public String toString() {
        return q.toString();
    }

    public Query addVariable(Node v) {
        q.addResultVar(v.getName());
        return this;
    }

    public Query setQueryStar() {
        q.setQueryResultStar(true);
        return this;
    }

    public Query addNamedGraphURI(Node_URI match) {
        q.addNamedGraphURI(match.getURI());
        return this;
    }

    public Query addGraphURI(Node_URI match) {
        q.addGraphURI(match.getURI());
        return this;
    }

    public Query addElement(Element sub) {
        q.setQueryPattern(sub);
        return this;
    }

    public Element getElement() {
        return q.getQueryPattern();
    }

    public TripleCollectorMark insert(TripleCollectorMark acc, Triple t) {
        acc.addTriple(acc.mark(), t);
        return acc;
    }

    public Query setBaseURI(String match) {
        q.getPrologue().setBaseURI(match);
        return this;
    }

    public Query setPrefix(Prefix pop) {
        System.out.println(pop.toString());
        q.getPrologue().setPrefix(pop.getPrefix(), pop.getUri());
        return this;
    }

    public Expr allocAggregate(Aggregator custom) {
        return q.allocAggregate(custom);

    }

    public Query addOrderBy(Object n) {
        return (n instanceof Node) ? addOrderBy((Node) n) : addOrderBy((Expr) n);
    }

    public Query addOrderBy(Node n) {
        q.addOrderBy(n, org.apache.jena.query.Query.ORDER_DEFAULT);
        return this;
    }

    public Query addOrderBy(Expr n) {
        q.addOrderBy(n, org.apache.jena.query.Query.ORDER_DEFAULT);
        return this;
    }

    public Query addOrderBy(Expr pop, String s) {
        q.addOrderBy(pop, "DESC".equals(s) ? orderDescending : orderAscending);
        return this;
    }

    public Query setLimit(String limit) {
        q.setLimit(Integer.parseInt(limit.trim()));
        return this;
    }

    public Query setOffset(String offset) {
        q.setOffset(Integer.parseInt(offset.trim()));
        return this;
    }
}