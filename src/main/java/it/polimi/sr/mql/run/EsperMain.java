package it.polimi.sr.mql.run;

import com.espertech.esper.client.*;
import com.espertech.esper.client.soda.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Riccardo on 16/08/16.
 */
public class EsperMain {

	public static void main(String[] args) {

		ConfigurationMethodRef ref = new ConfigurationMethodRef();
		Configuration cepConfig = new Configuration();
		cepConfig.getEngineDefaults().getLogging().setEnableExecutionDebug(true);
		cepConfig.getEngineDefaults().getLogging().setEnableTimerDebug(true);

		EPServiceProvider cep = EPServiceProviderManager.getProvider("", cepConfig);
		EPAdministrator cepAdm = cep.getEPAdministrator();
		EPRuntime cepRT = cep.getEPRuntime();

		cepAdm.createEPL("create map schema MyAEvent as (id string, loc int)");
		cepAdm.createEPL("create map schema MyBEvent as (id string, loc int)");

		EPStatement epl = cepAdm.createEPL("select * from pattern [a=MyAEvent -> b=MyBEvent(loc=a.loc, loc>35)]");

		epl.addListener(new UpdateListener() {
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {

				System.out.println("Standard EPL Statement");
				if (newEvents != null)
					for (EventBean newEvent : newEvents) {
						System.out.println(newEvent.getUnderlying().toString());
					}
				if (oldEvents != null)
					for (EventBean oldEvent : oldEvents) {
						System.out.println(oldEvent.getUnderlying().toString());
					}
			}
		});

		EPStatementObjectModel model = new EPStatementObjectModel();
		model.setSelectClause(SelectClause.createWildcard());

		PatternFilterExpr first = Patterns.filter("MyAEvent", "a");

		RelationalOpExpression loc = Expressions.eqProperty("loc", "a.loc");
		RelationalOpExpression loc1 = Expressions.ge("loc", 35);
		Conjunction and = Expressions.and(loc, loc1);

		PatternFilterExpr second = Patterns.filter(Filter.create("MyBEvent", and), "b");

		PatternExpr pattern = Patterns.followedBy(first, second);

		model.setFromClause(FromClause.create(PatternStream.create(pattern)));

		EPStatement epStatement = cepAdm.create(model);

		System.out.println(model.toEPL());
		epStatement.addListener(new UpdateListener() {
			public void update(EventBean[] newEvents, EventBean[] oldEvents) {

				System.out.println("API Built EPL Statement");
				if (newEvents != null)
					for (EventBean newEvent : newEvents) {
						System.out.println(newEvent.getUnderlying());
					}
				if (oldEvents != null)
					for (EventBean oldEvent : oldEvents) {
						System.out.println(oldEvent.getUnderlying());
					}
			}
		});

		Map<String, String> event = new HashMap<String, String>();
		event.put("id", "A");
		event.put("loc", "4");

		cepRT.sendEvent(event, "MyAEvent");

		event = new HashMap<String, String>();
		event.put("id", "B");
		event.put("loc", "3");

		cepRT.sendEvent(event, "MyBEvent");

		System.out.println("END");

	}
}
