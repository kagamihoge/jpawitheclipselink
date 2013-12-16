package jpawitheclipselink.entity;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * The persistent class for the INSERT_TO_MILLION_ROWS_TABLE2 database table.
 * 
 */
@Entity
@Table(name = "INSERT_TO_MILLION_ROWS_TABLE2")
public class InsertToMillionRowsTable2 implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "ENTITY_ID")
    private Integer entityId;

    @Column(name = "ENTITY_VALUE")
    private String entityValue;

    public InsertToMillionRowsTable2() {
    }

    public InsertToMillionRowsTable2(Integer entityId, String entityValue) {
        super();
        this.entityId = entityId;
        this.entityValue = entityValue;
    }

    public Integer getEntityId() {
        return this.entityId;
    }

    public void setEntityId(Integer entityId) {
        this.entityId = entityId;
    }

    public String getEntityValue() {
        return this.entityValue;
    }

    public void setEntityValue(String entityValue) {
        this.entityValue = entityValue;
    }

}