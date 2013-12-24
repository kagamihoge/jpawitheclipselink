package jpawitheclipselink.fetch;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.eclipse.persistence.jpa.JpaEntityManager;
import org.eclipse.persistence.queries.CursoredStream;
import org.eclipse.persistence.queries.DataReadQuery;
import org.eclipse.persistence.sessions.Session;

public class Main {

    public void go() {
        EntityManagerFactory emf = null;
        EntityManager em = null;
        try {
            emf = Persistence.createEntityManagerFactory("defaultPU");
            em = emf.createEntityManager();

            fetchMillionRows(em);
        } finally {
            em.close();
            emf.close();
        }
    }
    
    private void fetchMillionRows(EntityManager em) {
        JpaEntityManager eclipseLinkem = em.unwrap(JpaEntityManager.class);

        final String sql = "SELECT column_pk_int, column_vchar FROM million_rows_table";
        DataReadQuery q = new DataReadQuery(sql);
        q.useCursoredStream();
        q.setFetchSize(100);
        
        Session eclipseLinkSession = eclipseLinkem.getActiveSession();
        CursoredStream eclipseLinkCursor = (CursoredStream) eclipseLinkSession.executeQuery(q);
        
        int i=0;
        while (eclipseLinkCursor.hasNext()) {
            Object row = eclipseLinkCursor.next();
            i++;
            if (i % 10000 == 0) {
                eclipseLinkCursor.clear();
            }
        }
        System.out.println(i);

    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        new Main().go();

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

}
