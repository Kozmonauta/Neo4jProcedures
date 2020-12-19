package procedures;

import Utils.NodeUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

public class AttachNode {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    // Finds the closest client parent to hostNodeId
    private NodeWithParent getClosestClientParent(NodeWithParent hostNode, List<NodeWithParent> hosts, List<NodeWithParent> clients) {
        if (hostNode == null) return null;

        NodeWithParent cp = null;
        for (NodeWithParent c: clients) {
            if (hostNode.getId().equals(c.getAttachedId())) {
                cp = c;
                cp.setStatus(-1);
                break;
            }
        }

        if (cp == null) {
            cp = this.getClosestClientParent(NodeUtils.getNodeById(hostNode.getParentId(), hosts), hosts, clients);
        }

        return cp;
    }

    private List<NodeWithParent> getAffectedNodes(String hostNodeId, List<NodeWithParent> clients, List<NodeWithParent> hosts) {
        if (clients == null) return null;

        List<NodeWithParent> affectedNodes = new ArrayList<>();

        // The host node to which the new client node is attached
        NodeWithParent hostNode = NodeUtils.getNodeById(hostNodeId, hosts);

        NodeWithParent closestClientParent = this.getClosestClientParent(hostNode, hosts, clients);
        affectedNodes.add(closestClientParent);

        // Leaves under the client node
        List<NodeWithParent> clientLeaves = NodeUtils.getLeaves(clients);

        Iterator<NodeWithParent> it = clientLeaves.iterator();
        while (it.hasNext()) {
            NodeWithParent clientLeaf = it.next();
            NodeWithParent clientLeafHost = NodeUtils.getNodeById(clientLeaf.getAttachedId(), hosts);

            if (clientLeafHost == null) {
                it.remove();
            } else {
                NodeWithParent closestClientParentHost = NodeUtils.getNodeById(closestClientParent.getAttachedId(), hosts);
                List<NodeWithParent> hostAncestors = NodeUtils.collectAncestors(clientLeafHost, hosts, closestClientParentHost);
                if (!NodeUtils.isIncluded(hostNode, hostAncestors)) {
                    it.remove();
                }
            }
        }

        for (NodeWithParent nwp: clientLeaves) {
            List<NodeWithParent> clientAncestors = NodeUtils.collectAncestors(nwp, clients, closestClientParent);

            NodeWithParent topAncestor = clientAncestors.get(clientAncestors.size() - 1);
            if (closestClientParent.getId().equals(topAncestor.getParentId()) &&
                    !NodeUtils.isIncluded(topAncestor, affectedNodes)) {
                topAncestor.setStatus(1);
                affectedNodes.add(topAncestor);
            }
        }

        return affectedNodes;
    }

    private List<NodeWithParent> getClientNodes(Map<String, Object> hostData, Map<String, Object> clientData) {
        List<NodeWithParent> clientNodes = new ArrayList<>();
        String qFindClientNodes = "";

        qFindClientNodes += "MATCH (n: " + clientData.get("label") + ") WHERE ID(n) = " + clientData.get("id") + " ";
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
            Boolean allowMultipleClients = hostData.get("allowMultipleClients") != null ? (Boolean) hostData.get("allowMultipleClients") : false;

            while (resultClientNodes.hasNext()) {
                Map<String, Object> e = resultClientNodes.next();

                if (e.get("nsd") != null) {
                    Node node = (Node)e.get("nsd");
                    Node parent = (Node)e.get("npp");
                    Node host = (Node)e.get("h");

                    if (String.valueOf(hostData.get("id")).equals(String.valueOf(host.getId())) && allowMultipleClients == false) {
                        log.info("Warning: Already attached");
                        clientNodes.clear();
                        break;
                    }

                    NodeWithParent clientNode = new NodeWithParent(String.valueOf(node.getId()), parent != null ? String.valueOf(parent.getId()): null, String.valueOf(host.getId()));
                    clientNodes.add(clientNode);
                }
            }

            return null;
        });

        log.info("clientNodes: " + clientNodes);

        return clientNodes.isEmpty() ? null : clientNodes;
    }

    private List<NodeWithParent> getHostNodes(Map<String, Object> hostData) {
        List<NodeWithParent> hostNodes = new ArrayList<>();
        String qFindHostNodes = "";

        qFindHostNodes += "MATCH (n: " + hostData.get("label") + ") WHERE ID(n) = " + hostData.get("id") + " ";
        qFindHostNodes += "OPTIONAL MATCH (nps)-[:" + hostData.get("edgeLabel") + "*]->(n) ";
        qFindHostNodes += "OPTIONAL MATCH (n)-[:" + hostData.get("edgeLabel") + "*]->(ncs) ";
        qFindHostNodes += "WITH [n, nps, ncs] AS ns ";
        qFindHostNodes += "UNWIND ns AS nsu ";
        qFindHostNodes += "WITH DISTINCT nsu ";
        qFindHostNodes += "OPTIONAL MATCH (npp)-[:" + hostData.get("edgeLabel") + "]->(nsu) ";
        qFindHostNodes += "RETURN nsu, npp; ";

        log.info("qFindHostNodes: " + qFindHostNodes);

        db.beginTx();

        db.executeTransactionally(qFindHostNodes, new HashMap<String, Object>(), (ResultTransformer<ResultTransformer>) resultHostNodes -> {
            while (resultHostNodes.hasNext()) {
                Map<String, Object> e = resultHostNodes.next();

                if (e.get("nsu") != null) {
                    Node node = (Node)e.get("nsu");
                    Node parent = (Node)e.get("npp");

                    hostNodes.add(new NodeWithParent(String.valueOf(node.getId()), parent != null ? String.valueOf(parent.getId()): null));
                }
            }
            return null;
        });

        log.info("hostNodes: " + hostNodes);

        return hostNodes;
    }

    @Procedure(value = "dn.attachNode", mode = Mode.WRITE)
    @Description("attachNode")
    public void attachNode(@Name("hostData") Map<String, Object> hostData,
                                     @Name("clientData") Map<String, Object> clientData) {

        log.info("function: attachNode");

        List<NodeWithParent> hostNodes = this.getHostNodes(hostData);
        List<NodeWithParent> clientNodes = this.getClientNodes(hostData, clientData);

        List<NodeWithParent> affectedNodes = this.getAffectedNodes((String) hostData.get("id"), clientNodes, hostNodes);
        log.info("affectedNodes: " + affectedNodes);

        if (affectedNodes != null) {
            String qAttachNode = "";
            qAttachNode += "MATCH (hn:" + hostData.get("label") + ") WHERE ID(hn) = " + hostData.get("id") + " ";
            // Create new client node
            qAttachNode += "CREATE (n:" + clientData.get("label") + "";
            qAttachNode += "{";

            Map<String, Object> clientNodeData = (HashMap) clientData.get("data");
            for (String key: clientNodeData.keySet()) {
                Object value = clientNodeData.get(key);
                qAttachNode += key + ":";

                if (value instanceof String) {
                    qAttachNode += "\"" + String.valueOf(value) + "\"";
                } else
                if (value instanceof List<?>) {
                    ArrayList<Object> va = (ArrayList<Object>) value;
                    if (va.size() > 0) {
                        qAttachNode += "[";
                        for (Object v: va) {
                            qAttachNode += "\"" + String.valueOf(v) + "\"";
                            qAttachNode += ",";
                        }
                        // remove ","
                        qAttachNode = qAttachNode.substring(0, qAttachNode.length() - 1);
                        qAttachNode += "]";
                    }
                } else {
                    qAttachNode += "" + String.valueOf(value) + "";
                }
                qAttachNode += ",";
            }

            // remove ","
            qAttachNode = qAttachNode.substring(0, qAttachNode.length() - 1);

            qAttachNode += "}";
            qAttachNode +=  ")-[:" + clientData.get("attachEdgeLabel") + "]->(hn) ";
            qAttachNode += "WITH n ";

            NodeWithParent pn = null;
            for (NodeWithParent nwp: affectedNodes) {
                // distance < 0 -> parent node
                if (nwp.getStatus() < 0) {
                    pn = nwp;
                    qAttachNode += "MATCH (pn:" + clientData.get("label") + ") WHERE ID(pn) = " + nwp.getId() + " ";
                    // Connect parent client to new client node
                    qAttachNode += "CREATE (pn)-[:" + clientData.get("edgeLabel") + "]->(n) ";
                    qAttachNode += "WITH n,pn ";
                }
            }

            if (pn != null) {
                Integer i = 0;
                for (NodeWithParent nwp : affectedNodes) {
                    String nodeAlias = "an" + i;
                    // distance > 0 -> child node
                    if (nwp.getStatus() > 0) {
                        qAttachNode += "MATCH (" + nodeAlias + ":" + clientData.get("label") + ")<-[r_" + nodeAlias + ":" + clientData.get("edgeLabel") + "]-(pn) WHERE ID(" + nodeAlias + ") = " + nwp.getId() + " ";
                        // Connect new client node to child client nodes
                        qAttachNode += "CREATE (n)-[:" + clientData.get("edgeLabel") + "]->(" + nodeAlias + ") ";
                        // Delete relationships between parent and child nodes
                        qAttachNode += "DELETE r_" + nodeAlias + " ";
                        qAttachNode += "WITH n,pn ";
                    }
                    i++;
                }

                qAttachNode += "RETURN n;";

                log.info("qAttachNode: " + qAttachNode);
                db.executeTransactionally(qAttachNode);
            } else {
                log.info("Error: No parent node found");
            }
        }

    }

}