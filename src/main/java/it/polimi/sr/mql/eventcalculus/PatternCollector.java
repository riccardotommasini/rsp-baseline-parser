package it.polimi.sr.mql.eventcalculus;

import it.polimi.sr.mql.IFDecl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Riccardo on 17/08/16.
 */

@Getter
@Setter
@RequiredArgsConstructor
public class PatternCollector {

    private String operator;
    private Node var;
    private List<PatternCollector> patterns;
    private IFDecl ifdecl;
    private boolean bracketed = false;

    public PatternCollector(PatternCollector pop) {
        addPattern(pop);
        bracketed = true;

    }

    public boolean isVar() {
        return var != null;
    }

    public PatternCollector(IFDecl ifdecl, Node var) {
        this.var = var;
        this.ifdecl = ifdecl;
    }

    public void addPattern(PatternCollector p) {
        if (patterns == null)
            patterns = new ArrayList<PatternCollector>();
        patterns.add(p);
    }

    @Override
    public String toString() {
        String s = "";

        if (isVar()) {
            return var.getName();
        }

        if (operator != null && ("every".equals(operator.toLowerCase()) || "not".equals(operator.toLowerCase()))) {
            return operator + " (" + patterns.get(0) + ")";
        }

        if (operator == null && patterns.size() == 1) {
            s += bracketed ? "(" : "";
            s += patterns.get(0).toString();
            s += bracketed ? ")" : "";
            return s;
        }


        s += bracketed ? "(" : "";

        PatternCollector pc;
        for (int i = 0; i < patterns.size() - 1; i++) {
            pc = patterns.get(i);

            s += pc.toString();

            s += " " + operator + " ";
        }

        pc = patterns.get(patterns.size() - 1);

        s += pc.toString();

        s += bracketed ? ")" : "";

        return s;
    }

    public Set<Var> getJoinVariables() {
        Set<Var> joinVariables = null;
        for (PatternCollector pattern : patterns) {
            if (pattern.getIfdecl() != null && pattern.getIfdecl().getVars() != null) {
                if (joinVariables == null) {
                    joinVariables = pattern.getIfdecl().getVars();
                }
                joinVariables = pattern.getIfdecl().shared(joinVariables);
                joinVariables.retainAll(pattern.getJoinVariables());
            }
        }

        return joinVariables;
    }
}
