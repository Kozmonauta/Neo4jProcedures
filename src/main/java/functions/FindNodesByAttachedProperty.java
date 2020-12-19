package functions;

import Utils.NodeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import procedures.NodeWithParent;

import java.lang.reflect.Array;
import java.util.*;

public class FindNodesByAttachedProperty {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    private NodeWithParent findClosestClient(NodeWithParent hostNode, List<NodeWithParent> hosts, List<NodeWithParent> clients) {
        NodeWithParent c = NodeUtils.getNodeByAttachedId(hostNode.getId(), clients);
        if (c == null && hostNode.getParentId() != null) {
            return this.findClosestClient(NodeUtils.getNodeById(hostNode.getParentId(), hosts), hosts, clients);
        } else {
            return c;
        }
    }

    private void fillNodeInheritProperties(NodeWithParent node, String propertyName, List<NodeWithParent> clientNodes, Boolean check) {
//        log.info("fillNodeInheritProperties: " + node);
        Object pv = node.getProperties().get(propertyName);
//        log.info("pv: " + pv.toString());
//        log.info("monode: " + node);

        // traverse client node's children
        for (NodeWithParent nwp: clientNodes) {
//            log.info("nwp.getParentId(): " + nwp.getParentId());
            if (node.getId().equals(nwp.getParentId())) {
//                log.info("equals: nwp.getProperties(): " + nwp.getProperties());
                Object cpv = null;
                if (nwp.getProperties() != null) {
                    cpv = nwp.getProperties().get(propertyName);
                }

                nwp.getProperties().put(propertyName, NodeUtils.mergeInheritProperty(pv, cpv));

                if (check == false && nwp.getStatus() != null && nwp.getStatus() == 1) {
                    check = true;
                }

                if (check) {
                    nwp.setStatus(1);
                }

                this.fillNodeInheritProperties(nwp, propertyName, clientNodes, check);
            }
        }
    }

    private void checkHost(NodeWithParent hostNode, String propertyName, Object propertyValue, List<NodeWithParent> hosts, List<NodeWithParent> clients) {
        NodeWithParent c = this.findClosestClient(hostNode, hosts, clients);

        if (c.getStatus() != null && c.getStatus() == 1) {
            Object pv = c.getProperties().get(propertyName);

            if (pv != null) {
                if (pv instanceof Object[]) {
                    List<Object> pvl = Arrays.asList((Object[]) pv);
                    if (pvl.contains(propertyValue)) {
                        hostNode.setStatus(2);
                    }
                } else
                if (pv instanceof Object) {
                    if (propertyValue.equals(pv)) {
                        hostNode.setStatus(2);
                    }
                }
            }
        }

        for (NodeWithParent nwp: hosts) {
            if (hostNode.getId().equals(nwp.getParentId())) {
                this.checkHost(nwp, propertyName, propertyValue, hosts, clients);
            }
        }
    }

    @UserFunction(value = "dn.findNodesByAttachedProperty")
    @Description("dn.findNodesByAttachedProperty()")
    public List<String> findNodesByAttachedProperty(
            @Name("hostData") Map<String, Object> hostData,
            @Name("clientData") Map<String, Object> clientData) {

        log.info("function: findNodesByAttachedProperty");

        String hostNodeId = (String) hostData.get("id");
        String clientNodeId = (String) clientData.get("id");
        String inheritPropertyName = (String) clientData.get("propertyName");
        Object inheritPropertyValue = clientData.get("propertyValue");

        String qFindHostNodes = "";
        qFindHostNodes += "MATCH (n: " + hostData.get("label") + ") WHERE ID(n) = " + hostNodeId + " ";
        qFindHostNodes += "OPTIONAL MATCH (nps)-[:" + hostData.get("edgeLabel") + "*]->(n) ";
        qFindHostNodes += "OPTIONAL MATCH (n)-[:" + hostData.get("edgeLabel") + "*]->(ncs) ";
        qFindHostNodes += "WITH [n, nps, ncs] AS ns ";
        qFindHostNodes += "UNWIND ns AS nsu ";
        qFindHostNodes += "WITH DISTINCT nsu ";
        qFindHostNodes += "OPTIONAL MATCH (npp)-[:" + hostData.get("edgeLabel") + "]->(nsu) ";
        qFindHostNodes += "RETURN nsu, npp; ";

        log.info("qFindHostNodes: " + qFindHostNodes);

        List<String> ret = new ArrayList<>();

        db.beginTx();
        db.executeTransactionally(qFindHostNodes, new HashMap<String, Object>(), (ResultTransformer<ResultTransformer>) resultHostNodes -> {
            // hostNode status:
            // 1: is queried hostNode
            // 2: has to be returned
            List<NodeWithParent> hostNodes = new ArrayList<>();

            while (resultHostNodes.hasNext()) {
                Map<String, Object> e = resultHostNodes.next();

                if (e.get("nsu") != null) {
                    Node node = (Node) e.get("nsu");
                    Node parent = (Node) e.get("npp");

                    NodeWithParent nwp = new NodeWithParent();
                    nwp.setId(String.valueOf(node.getId()));
                    nwp.setParentId(parent != null ? String.valueOf(parent.getId()) : null);

                    hostNodes.add(nwp);
                }
            }

            String qFindClientNodes = "";
            qFindClientNodes += "MATCH (n: " + clientData.get("label") + ") WHERE ID(n) = " + clientNodeId + " ";
            qFindClientNodes += "OPTIONAL MATCH (nps)-[:" + clientData.get("edgeLabel") + "*]->(n) ";
            qFindClientNodes += "OPTIONAL MATCH (n)-[:" + clientData.get("edgeLabel") + "*]->(ncs) ";
            qFindClientNodes += "WITH [n, nps, ncs] AS ns ";
            qFindClientNodes += "UNWIND ns AS nsd ";
            qFindClientNodes += "MATCH (nsd)-[:" + clientData.get("attachEdgeLabel") + "]->(h:" + hostData.get("label") + ") ";
            qFindClientNodes += "WITH DISTINCT nsd, h ";
            qFindClientNodes += "OPTIONAL MATCH (npp)-[:" + clientData.get("edgeLabel") + "]->(nsd) ";
            qFindClientNodes += "RETURN nsd, npp, h; ";

            log.info("qFindClientNodes: " + qFindClientNodes);

            db.executeTransactionally(qFindClientNodes, new HashMap<String, Object>(), (ResultTransformer<ResultTransformer>) resultClientNodes -> {
                List<NodeWithParent> clientNodes = new ArrayList<>();

                while (resultClientNodes.hasNext()) {
                    Map<String, Object> e = resultClientNodes.next();

                    if (e.get("nsd") != null) {
                        Node node = (Node) e.get("nsd");
                        Node parent = (Node) e.get("npp");
                        Node host = (Node) e.get("h");

                        String nodeId = String.valueOf(node.getId());

                        NodeWithParent clientNode = new NodeWithParent(nodeId, parent != null ? String.valueOf(parent.getId()) : null, String.valueOf(host.getId()));
                        clientNode.setProperties(new HashMap<>());

                        if (node.hasProperty(inheritPropertyName)) {
//                            log.info("hasProperty");
                            clientNode.getProperties().put(inheritPropertyName, node.getProperty(inheritPropertyName));
                        }

                        if (nodeId.equals(clientNodeId)) {
                            clientNode.setStatus(1);
                        }

//                        log.info("clientNode.getProperties(): " + clientNode.getProperties().get(inheritPropertyName));
                        clientNodes.add(clientNode);
                    }
                }

//                log.info("clientNodes: " + clientNodes);
                NodeWithParent topClient = NodeUtils.getNodeByParentId(null, clientNodes);
//                log.info("topClient: " + topClient);
                // each client node will have the calculated inherited property value
                this.fillNodeInheritProperties(topClient, inheritPropertyName, clientNodes,  topClient.getStatus() != null);
//                log.info("filled clients: " + clientNodes);

                this.checkHost(NodeUtils.getNodeById(hostNodeId, hostNodes), inheritPropertyName, inheritPropertyValue, hostNodes, clientNodes);

                for (NodeWithParent nwp: hostNodes) {
                    if (nwp.getStatus() != null && nwp.getStatus() == 2) {
                        ret.add(nwp.getId());
                    }
                }

                return null;
            });

            return null;
        });

        log.info("ret: " + ret);

        return ret;
    }
}
