package eu.supersede.mdm.storage.db.jena;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class JenaConnectionServlet implements ServletContextListener {

    public void contextInitialized(ServletContextEvent sce) {
        JenaConnection conn =  JenaConnection.getInstance();
        conn.init();
    }

    public void contextDestroyed(ServletContextEvent sce) {
        JenaConnection conn = JenaConnection.getInstance();
        conn.close();
    }

}
