package com.blazingdb.calcite.catalog.repository;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchema;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;

public class DatabaseRepository {

	
	private static SessionFactory getSessionFactory() {
		Configuration configuration = new Configuration().configure();
		SessionFactory sessionFactory = configuration.buildSessionFactory();
		return sessionFactory;
	}
	

	public void createDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		Session sessionObj = null;
		try {
			sessionObj = getSessionFactory().openSession();
			transObj = sessionObj.beginTransaction();

			sessionObj.persist(database);
			
			transObj.commit();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} finally {
			sessionObj.close(); 
		}
	}
	
	
	public void dropDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		Session sessionObj = null;
		try {
			sessionObj = getSessionFactory().openSession();
			transObj = sessionObj.beginTransaction();

			
			sessionObj.delete(database);
			
			transObj.commit();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} finally {
			sessionObj.close(); 
		}
	}
	
	public void createTable(String tableName, List<String> columnNames, )
}
