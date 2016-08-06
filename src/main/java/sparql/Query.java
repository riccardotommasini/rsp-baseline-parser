package sparql;

import lombok.Data;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.TripleCollectorMark;
import org.apache.jena.graph.Node;
/**
 * Created by Riccardo on 05/08/16.
 */
@Data
public class Query {

    private ElementGroup where;
    private org.apache.jena.query.Query q;


    public Query() {
        this.q = new org.apache.jena.query.Query();
        this.q.setQueryPattern(this.where = new ElementGroup());
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

    Element elm = null;

    public Query addElement(Element sub) {
        ((ElementGroup) q.getQueryPattern()).addElement(sub);
        return this;
    }

    public TripleCollectorMark insert(TripleCollectorMark acc, Triple t) {
        acc.addTriple(acc.mark(), t);
        return acc;
    }

}