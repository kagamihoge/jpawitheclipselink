package jpawitheclipselink.batchwriting;

import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import jpawitheclipselink.entity.InsertToMillionRowsTable2;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.persistence.config.BatchWriting;
import org.eclipse.persistence.config.PersistenceUnitProperties;

public class Main {

    public void go() {
        EntityManagerFactory emf = null;
        EntityManager em = null;
        try {
            emf = Persistence.createEntityManagerFactory("defaultPU", createProps());
            // emf = Persistence.createEntityManagerFactory("defaultPU");
            em = emf.createEntityManager();

            insertMillionRows(em);
        } finally {
            em.close();
            emf.close();
        }
    }

    @SuppressWarnings("all")
    private Map createProps() {
        Map props = new HashMap();

        props.put(PersistenceUnitProperties.BATCH_WRITING, BatchWriting.OracleJDBC);
        props.put(PersistenceUnitProperties.BATCH_WRITING_SIZE, "100");

        return props;
    }

    private void insertMillionRows(EntityManager em) {
        EntityTransaction tx = em.getTransaction();

        tx.begin();
        try {
            for (int i = 1; i <= 1_000_000; i++) {
                InsertToMillionRowsTable2 newRecord = new InsertToMillionRowsTable2(i,
                    RandomStringUtils.randomAlphanumeric(20));
                em.persist(newRecord);
                if (i % 100 == 0) {
                    em.flush();
                    em.clear();
                }
            }
        } finally {
            tx.commit();
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        new Main().go();

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

}
