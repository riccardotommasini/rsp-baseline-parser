package it.polimi.sr.mql.events.calculus;

import com.espertech.esper.client.soda.*;
import it.polimi.sr.mql.events.declaration.IFDecl;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Riccardo on 17/08/16.
 */
public class MatchClause {

	@Override
	public String toString() {
		return "MatchClause{" + "expr=" + expr + '}';
	}

	private PatternCollector expr;

	public MatchClause(PatternCollector pop) {
		this.expr = pop;
	}

	public Set<Var> getJoinVariables() {
		Set<Var> joinVariables = null;
		for (IFDecl ifDecl : getIfDeclarations()) {
			if (joinVariables == null) {
				joinVariables = new HashSet<Var>(ifDecl.getVars());
			}
			joinVariables.retainAll(ifDecl.getVars());
		}
		return joinVariables;
	}

	public List<IFDecl> getIfDeclarations() {
		List<IFDecl> ifDeclarations = new ArrayList<IFDecl>();
		if (expr.getIfdecl() != null) {
			ifDeclarations.add(expr.getIfdecl());
		}
		ifDeclarations.addAll(expr.getIfDeclarations());
		return ifDeclarations;

	}

	public EPStatementObjectModel toEpl() {
		EPStatementObjectModel model = new EPStatementObjectModel();
		model.setSelectClause(SelectClause.createWildcard());
		PatternExpr pattern = expr.toEPL(getIfDeclarations());
		model.setFromClause(FromClause.create(PatternStream.create(pattern)));
		return model;
	}
}
