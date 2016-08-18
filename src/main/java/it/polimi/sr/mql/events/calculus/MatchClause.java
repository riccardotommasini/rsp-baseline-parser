package it.polimi.sr.mql.events.calculus;

import com.espertech.esper.client.soda.*;
import org.apache.jena.sparql.core.Var;

import java.util.Set;

/**
 * Created by Riccardo on 17/08/16.
 */
public class MatchClause {


    @Override
    public String toString() {
        return "MatchClause{" +
                "expr=" + expr +
                '}';
    }

    private PatternCollector expr;

    public MatchClause(PatternCollector pop) {
        this.expr = pop;
    }


    public Set<Var> getJoinVariables() {
        if (expr.getIfdecl() != null) {
            return expr.getIfdecl().getVars();
        }
        return expr.getJoinVariables();
    }

    public EPStatementObjectModel toEpl() {
        EPStatementObjectModel model = new EPStatementObjectModel();
        model.setSelectClause(SelectClause.createWildcard());
        PatternExpr pattern = expr.toEPL();
        model.setFromClause(FromClause.create(PatternStream.create(pattern)));
        return model;
    }
}
