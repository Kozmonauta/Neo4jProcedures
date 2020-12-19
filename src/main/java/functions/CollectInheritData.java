package functions;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import Utils.NodeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import procedures.NodeWithParent;

public class CollectInheritData {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @UserFunction(value = "dn.collectInheritData")
    @Description("dn.collectInheritData()")
    public Map<String, Object> collectInheritData(
            @Name("nodeId") String nodeId,
            @Name("nodeLabel") String nodeLabel,
            @Name("edgeLabel") String edgeLabel,
            @Name("edgeDirection") String edgeDirection,
            @Name("propertyName") String propertyName) {

        log.info("function: collectInheritData");

        String qCollectPropertyValues = "";

        if (nodeLabel != null) {
            qCollectPropertyValues += "MATCH (n: " + nodeLabel + ") WHERE ID(n) = " + nodeId + " ";

            if ("out".equals(edgeDirection)) {
                qCollectPropertyValues += "OPTIONAL MATCH (n)-[:" + edgeLabel + "*]->(p:" + nodeLabel + ") ";
            } else {
                qCollectPropertyValues += "OPTIONAL MATCH (n)<-[:" + edgeLabel + "*]-(p:" + nodeLabel + ") ";
            }
        } else {
            qCollectPropertyValues += "MATCH (n) WHERE ID(n) = " + nodeId + " ";

            if ("out".equals(edgeDirection)) {
                qCollectPropertyValues += "OPTIONAL MATCH (n)-[:" + edgeLabel + "*]->(p) ";
            } else {
                qCollectPropertyValues += "OPTIONAL MATCH (n)<-[:" + edgeLabel + "*]-(p) ";
            }
        }

        qCollectPropertyValues += "WITH [n, p] AS np ";
        qCollectPropertyValues += "UNWIND np AS npu ";
        qCollectPropertyValues += "RETURN DISTINCT npu;";

        log.info("qCollectPropertyValues: " + qCollectPropertyValues);

        Map<String, Object> ret = new HashMap<>();

        db.beginTx();
        db.executeTransactionally(qCollectPropertyValues, new HashMap<String, Object>(), (ResultTransformer<ResultTransformer>) resultNodes -> {
            while (resultNodes.hasNext()) {
                Map<String, Object> e = resultNodes.next();

                // iterate the nodes from child to ancestors
                for (Map.Entry<String, Object> nodeEntry: e.entrySet()) {
                    Node node = (Node) nodeEntry.getValue();

                    if (node != null) {
                        if (propertyName != null) {
                            if (node.hasProperty(propertyName)) {
                                Map<String, Object> pd = new HashMap<>();
                                pd.put(propertyName, node.getProperty(propertyName));

                                NodeUtils.mergeInheritData(ret, pd);
                            }
                        } else {
                            NodeUtils.mergeInheritData(ret, node.getAllProperties());
                        }
                    }
                }
            }

            return null;
        });

        log.info("ret: " + ret);

        return ret;
    }

}
