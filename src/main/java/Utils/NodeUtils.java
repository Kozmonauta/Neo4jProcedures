package Utils;

import org.json.JSONException;
import org.json.JSONObject;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import procedures.NodeWithParent;

import java.util.*;

public final class NodeUtils {

    private static NodeUtils instance = new NodeUtils();

    private NodeUtils() {}

    public static NodeUtils getInstance() {
        return instance;
    }

    public static NodeWithParent getNodeById(String id, List<NodeWithParent> nodes) {
        NodeWithParent node = null;

        for (NodeWithParent n: nodes) {
            if (n.getId().equals(id)) {
                node = n;
                break;
            }
        }

        return node;
    }

    public static NodeWithParent getNodeByAttachedId(String attachedId, List<NodeWithParent> nodes) {
        NodeWithParent node = null;

        for (NodeWithParent n: nodes) {
            if (n.getAttachedId().equals(attachedId)) {
                node = n;
                break;
            }
        }

        return node;
    }

    public static NodeWithParent getNodeByParentId(String parentId, List<NodeWithParent> nodes) {
        NodeWithParent node = null;

        for (NodeWithParent n: nodes) {
            if (parentId != null) {
                if (n.getParentId().equals(parentId)) {
                    node = n;
                    break;
                }
            } else {
                if (n.getParentId() == null) {
                    node = n;
                    break;
                }
            }
        }

        return node;
    }

    public static Boolean isIncluded(NodeWithParent node, List<NodeWithParent> nodes) {
        Boolean ret = false;

        for (NodeWithParent nwp: nodes) {
            if (nwp.getId().equals(node.getId())) {
                ret = true;
            }
        }

        return ret;
    }

    public static List<NodeWithParent> collectAncestors(NodeWithParent node, List<NodeWithParent> nodes, NodeWithParent limit) {
        List<NodeWithParent> ancestors = new ArrayList<>();
        NodeWithParent actNode = node;
        Boolean limitReached = false;

        do {
            ancestors.add(actNode);
            if (actNode.getParentId() == null) break;
            actNode = getNodeById(actNode.getParentId(), nodes);

            if (limit != null && limit.getId() == actNode.getId()) limitReached = true;
        }
        while (actNode.getParentId() != null && !limitReached);

        return ancestors;
    }

    public static List<NodeWithParent> getLeaves(List<NodeWithParent> nodes) {
        List<NodeWithParent> leaves = new ArrayList<>();

        for (NodeWithParent nwp: nodes) {
            Boolean isLeaf = true;

            for (NodeWithParent nwp2: nodes) {
                if (nwp.getId().equals(nwp2.getParentId())) {
                    isLeaf = false;
                }
            }

            if (isLeaf) {
                leaves.add(nwp);
            }
        }

        return leaves;
    }

    public static Integer[] getIntegerArray(int[] ia) {
        Integer[] integerArray = new Integer[ia.length];
        for (int i=0; i<ia.length; i++) {
            integerArray[i] = ia[i];
        }
        return integerArray;
    }

    public static Long[] getLongArray(long[] ia) {
        Long[] longArray = new Long[ia.length];
        for (int i=0; i<ia.length; i++) {
            longArray[i] = ia[i];
        }
        return longArray;
    }

    public static List mergeLists(List l1, List l2) {
        if (l1 == null) {
            return l2;
        }

        for (Object o2: l2) {
            if (!l1.contains(o2)) {
                l1.add(o2);
            }
        }

        return l1;
    }

    public static Object[] mergeArrays(Object[] a1, Object[] a2) {
        if (a1 == null) {
            return a2;
        }

        List<Object> a1List = new ArrayList<>(Arrays.asList(a1));
        List<Object> a2List = new ArrayList<>(Arrays.asList(a2));

        for (Object n: a2List) {
            if (!a1List.contains(n)) {
                a1List.add(n);
            }
        }

        return a1List.toArray();
    }

    public static Map<String, Object> mergeMaps(Map<String, Object> m1, Map<String, Object> m2) {
        Iterator<String> i2 = m2.keySet().iterator();

        while (i2.hasNext()) {
            String key2 = i2.next();
            Object val2 = m2.get(key2);

            if (!m1.containsKey(key2)) {
                m1.put(key2, val2);
            } else {
                if (val2 instanceof List<?>) {
                    m1.put(key2, mergeLists((List) m1.get(key2), (List) val2));
                } else
                if (val2 instanceof Map) {
                    m1.put(key2, mergeMaps((Map) m1.get(key2), (Map) val2));
                }
            }
        }

        return m1;
    }

    public static Map<String, Object> getJSONMap(String value) {
        Map<String, Object> jsonMap = null;
        JSONObject json = null;

        try {
            json = new JSONObject((String) value);
            jsonMap = json.toMap();
        } catch (JSONException error) {
            // Not valid json
            return null;
        }

        return jsonMap;
    }

    public static Object mergeInheritProperty(Object data, Object newData) {
        // int[] -> Integer[]
        if (newData instanceof int[]) {
            newData = getLongArray((long[]) newData);
//            newData = getIntegerArray((int[]) newData);
        }

        if (data instanceof int[]) {
            data = getIntegerArray((int[]) data);
        }

        // if String value is supposed to be a JSON then convert to JSON Map
        if (newData instanceof String) {
            String pvs = (String) newData;

            if ("{\"".equals((pvs).substring(0, 2))) {
                newData = getJSONMap(pvs);
            }
        }

        if (data == null) {
            data = newData;
        } else {
            if (newData instanceof Object[]) {
                data = mergeArrays((Object[]) data, (Object[]) newData);
            } else

            if (newData instanceof Map) {
                data = mergeMaps((Map) data, (Map) newData);
            }
        }

        return data;
    }

    public static Map<String, Object> mergeInheritData(Map<String, Object> data, Map<String, Object> newData) {
        // iterate the node's properties
        for (Map.Entry<String, Object> propertyEntry: newData.entrySet()) {
            String pk = propertyEntry.getKey();
            Object pv = propertyEntry.getValue();

            data.put(pk, mergeInheritProperty(data.get(pk), pv));
        }

        return data;
    }

}
