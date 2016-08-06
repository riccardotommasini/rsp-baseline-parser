package sparql;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.syntax.ElementPathBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Riccardo on 06/08/16.
 */
public class TripleBuilder implements Cloneable {

    public Node s;
    public Map<Node, List<Node>> pos;

    public TripleBuilder(Node s) {
        this.s = s;
        this.pos = new HashMap<Node, List<Node>>();
    }

    public TripleBuilder(Node p, Node s) {
        this.s = s;
        this.pos = new HashMap<Node, List<Node>>();
        this.pos.put(p, new ArrayList<Node>());
    }

    public Node add(Node p) {
        if (pos.containsKey(p)) {
            new RuntimeException("Verb Exisits");
        } else {
            pos.put(p, new ArrayList<Node>());
        }
        return p;
    }

    public Node add(Node o, Node p) {
        List<Node> nodes = null;
        if (pos.containsKey(p)) {
            nodes = pos.get(p);
        } else {
            nodes = new ArrayList<Node>();
        }

        nodes.add(o);
        pos.put(p, nodes);
        return p;
    }

    public ElementPathBlock build() {
        ElementPathBlock tempAcc = new ElementPathBlock();
        for (Node p : pos.keySet()) {
            for (Node o : pos.get(p)) {
                tempAcc.addTriple(new Triple(s, p, o));
            }
        }
        return tempAcc;
    }
}
