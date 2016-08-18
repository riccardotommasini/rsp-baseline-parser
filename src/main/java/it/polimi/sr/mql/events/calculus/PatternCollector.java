package it.polimi.sr.mql.events.calculus;

import com.espertech.esper.client.soda.*;
import it.polimi.sr.mql.events.declaration.IFDecl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Riccardo on 17/08/16.
 */

@Getter
@Setter
@RequiredArgsConstructor
public class PatternCollector {
    final private String regex = "([0-9]+)\\s*(ms|s|m|h|d)";
    final private Pattern p = Pattern.compile(regex);

    private String operator;
    private String var;
    private List<PatternCollector> patterns;
    private IFDecl ifdecl;
    private boolean bracketed = false;

    public PatternCollector(PatternCollector pop) {
        addPattern(pop);
        bracketed = true;
    }


    public PatternCollector(String match, PatternCollector pop) {
        operator = "WITHIN";
        PatternCollector var = new PatternCollector();
        var.setVar(match);
        if (patterns == null)
            patterns = new ArrayList<PatternCollector>();
        patterns.add(pop);
        patterns.add(var);

    }

    public boolean isVar() {
        return var != null;
    }

    public PatternCollector(IFDecl ifdecl, Node var) {
        this.var = var.getName();
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
            return var;
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

    public TimePeriodExpression toTimeExpr() {
        Matcher matcher = p.matcher(var);
        if (matcher.find()) {
            MatchResult res = matcher.toMatchResult();
            if ("ms".equals(res.group(2))) {
                return Expressions.timePeriod(null, null, null, null, Integer.parseInt(res.group(1)));
            } else if ("s".equals(res.group(2))) {
                return Expressions.timePeriod(null, null, null, Integer.parseInt(res.group(1)), null);
            } else if ("m".equals(res.group(2))) {
                return Expressions.timePeriod(null, null, Integer.parseInt(res.group(1)), null, null);
            } else if ("h".equals(res.group(2))) {
                return Expressions.timePeriod(null, Integer.parseInt(res.group(1)), null, null, null);
            } else if ("d".equals(res.group(2))) {
                return Expressions.timePeriod(Integer.parseInt(res.group(1)), null, null, null, null);
            }
        }
        return null;
    }

    public PatternExpr toEPL() {

        if (isVar()) {
            return Patterns.filter(var, var);
        }

        if ((operator == null || operator.isEmpty()) && patterns.size() == 1) {
            return patterns.get(0).toEPL();
        }

        PatternExpr pattern = null;
        if (operator != null) {

            if ("within".equals(operator.toLowerCase())) {
                TimePeriodExpression timeExpr = patterns.get(1).toTimeExpr();
                return Patterns.guard("timer", "within",
                        new Expression[]{timeExpr},
                        patterns.get(0).toEPL());
            } else if ("every".equals(operator.toLowerCase())) {
                return Patterns.every(patterns.get(0).toEPL());
            } else if ("not".equals(operator.toLowerCase())) {
                return Patterns.not(patterns.get(0).toEPL());
            } else if ("->".equals(operator.toLowerCase())) {
                pattern = Patterns.followedBy();
                for (PatternCollector p : patterns) {
                    ((PatternFollowedByExpr) pattern).add(p.toEPL());
                }
            } else if ("or".equals(operator.toLowerCase())) {
                pattern = Patterns.or();
                for (PatternCollector p : patterns) {
                    ((PatternOrExpr) pattern).add(p.toEPL());
                }
            } else if ("and".equals(operator.toLowerCase())) {
                pattern = Patterns.and();
                for (PatternCollector p : patterns) {
                    ((PatternAndExpr) pattern).add(p.toEPL());
                }

            }
        }

        return pattern;

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
