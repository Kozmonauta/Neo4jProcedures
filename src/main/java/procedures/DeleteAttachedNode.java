package procedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;

public class DeleteAttachedNode {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "dn.deleteAttachedNode", mode = Mode.WRITE)
    @Description("deleteAttachedNode")
    public void deleteAttachedNode(@Name("hostData") Map<String, Object> hostData,
                                   @Name("clientData") Map<String, Object> clientData) {

        log.info("function: deleteAttachedNode");

        String qFindClientNode = "";
        qFindClientNode += "MATCH (n: " + clientData.get("label") + ")-[hr:" + clientData.get("attachEdgeLabel") + "]->(h:" + hostData.get("label") + ") WHERE ID(n) = " + clientData.get("id") + " ";
        qFindClientNode += "OPTIONAL MATCH (n)<-[pr:" + clientData.get("edgeLabel") + "]-(p:" + clientData.get("label") + ") ";
        qFindClientNode += "OPTIONAL MATCH (n)-[cr:" + clientData.get("edgeLabel") + "]->(c:" + clientData.get("label") + ") ";
        qFindClientNode += "RETURN n,h,p,c,hr,pr,cr; ";

        log.info("qFindClientNode: " + qFindClientNode);

        db.beginTx();
        db.executeTransactionally(qFindClientNode, new HashMap<String, Object>(), (ResultTransformer<ResultTransformer>) resultClientNode -> {
            Map<String, Object> result = null;
            while (resultClientNode.hasNext()) {
                result = resultClientNode.next();
            }

            if (result == null) {
                log.info("Error: No result");
                return null;
            }

            if (result.get("n") == null) {
                log.info("Error: Node not found");
                return null;
            }

            String qDeleteNode = "";
            qDeleteNode += "MATCH (n: " + clientData.get("label") + ")-[hr:" + clientData.get("attachEdgeLabel") + "]->(h:" + hostData.get("label") + ") WHERE ID(n) = " + clientData.get("id") + " ";
            if (result.get("p") != null) {
                qDeleteNode += "MATCH (n)<-[pr:" + clientData.get("edgeLabel") + "]-(p:" + clientData.get("label") + ") ";
            }
            if (result.get("c") != null) {
                qDeleteNode += "MATCH (n)-[cr:" + clientData.get("edgeLabel") + "]->(c:" + clientData.get("label") + ") ";
            }
            if (result.get("p") != null && result.get("c") != null) {
                qDeleteNode += "CREATE (p)-[:" + clientData.get("edgeLabel") + "]->(c) ";
            }
            qDeleteNode += "DELETE hr,";
            if (result.get("p") != null) {
                qDeleteNode += "pr,";
            }
            if (result.get("c") != null) {
                qDeleteNode += "cr,";
            }
            qDeleteNode += "n ";
            qDeleteNode += "RETURN n; ";

            log.info("qDeleteNode: " + qDeleteNode);
            db.executeTransactionally(qDeleteNode);

            return null;
        });
    }

}