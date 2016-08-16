package it.polimi.sr;

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


        cepAdm.createEPL("create map schema MyAEvent as (id string, loc String)");
        cepAdm.createEPL("create map schema MyBEvent as (id string, loc String)");


        EPStatement epl = cepAdm.createEPL("select * from pattern [every a=MyAEvent -> MyBEvent(loc=a.loc)]");

        epl.addListener(new UpdateListener() {
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {

                System.out.println("Standard EPL Statement");
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


        EPStatementObjectModel model = new EPStatementObjectModel();
        model.setSelectClause(SelectClause.createWildcard());

        PatternEveryExpr first = Patterns.everyFilter("MyAEvent", "a");
        PatternFilterExpr second = Patterns.filter(Filter.create("MyBEvent", Expressions.eqProperty("loc", "a.loc")));

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
        event.put("loc", "loc");

        cepRT.sendEvent(event, "MyAEvent");

        event = new HashMap<String, String>();
        event.put("id", "B");
        event.put("loc", "loc");

        cepRT.sendEvent(event, "MyBEvent");

        System.out.println("END");

    }
}
