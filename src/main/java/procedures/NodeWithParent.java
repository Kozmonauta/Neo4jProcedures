package procedures;

import java.util.Map;

public class NodeWithParent {

    private String id;
    private String parentId;
    private String attachedId;
    private Integer status;
    private Map<String, Object> properties;

    public NodeWithParent() {}

    public NodeWithParent(String id, String parentId) {
        this.id = id;
        this.parentId = parentId;
    }

    public NodeWithParent(String id, String parentId, String attachedId) {
        this.id = id;
        this.parentId = parentId;
        this.attachedId = attachedId;
    }

    @Override
    public String toString() {
        return "NodeWithParent{" +
                "id='" + id + '\'' +
                ", parentId='" + parentId + '\'' +
                ", attachedId='" + attachedId + '\'' +
                ", status=" + status + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getAttachedId() {
        return attachedId;
    }

    public void setAttachedId(String attachedId) {
        this.attachedId = attachedId;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

}
