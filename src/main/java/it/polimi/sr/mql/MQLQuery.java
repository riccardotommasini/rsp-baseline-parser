package it.polimi.sr.mql;

import it.polimi.sr.mql.events.calculus.MatchClause;
import it.polimi.sr.mql.events.declaration.EventDecl;
import it.polimi.sr.mql.events.declaration.IFDecl;
import it.polimi.sr.rsp.RSPQuery;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.jena.graph.Node;
import org.apache.jena.query.QueryBuildException;
import org.apache.jena.query.QueryException;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Riccardo on 05/08/16.
 */
@Data
@NoArgsConstructor
public class MQLQuery extends RSPQuery {

    private Map<String, EventDecl> eventDeclarations;
    private boolean MQLQyeryStar, emitQuery;
    protected VarExprList MQLprojectVars = new VarExprList();
    private boolean MQLresultVarsSet;
    private List<MatchClause> matchclauses;


    public MQLQuery(Prologue prologue) {
        super(prologue);
    }

    public RSPQuery getRSPQuery() {
        return this;
    }


    @Override
    public MQLQuery addElement(ElementGroup sub) {
        setQueryPattern(sub);

        // TODO UNION?
        if (this.isEmitQuery()) {
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
            setQConstructTemplate(template);
        }
        return this;
    }

    @Override
    public String toString() {
        return super.toString();
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
                throw new QueryBuildException(
                        "Duplicate variable (had an expression) in result projection '" + v + "'");
        }
        varlist.add(v);
        return this;
    }

    public MQLQuery setMQLQueryStar() {
        this.MQLQyeryStar = true;
        return this;
    }

    public void addMatchClause(MatchClause matchClause) {
        if (matchclauses == null)
            matchclauses = new ArrayList<MatchClause>();
        this.matchclauses.add(matchClause);
    }

    public IFDecl getIfClause(Node peek) {
        EventDecl eventDecl = eventDeclarations.get(peek.getName());
        return eventDecl != null ? eventDecl.getIfdecl() : null;
    }

    public MQLQuery setEmitQuery() {
        setQueryConstructType();
        emitQuery = true;
        return this;
    }
}