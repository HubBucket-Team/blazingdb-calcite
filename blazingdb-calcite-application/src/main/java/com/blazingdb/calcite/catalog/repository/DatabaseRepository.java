package com.blazingdb.calcite.catalog.repository;

import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchema;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;

public class DatabaseRepository {

	private Session sessionObj = null;
	
	public DatabaseRepository() {
		sessionObj = getSessionFactory().openSession();
	}
	
	  @Override
	  public void finalize() {
	   sessionObj.close();
	  }
	
	private static SessionFactory getSessionFactory() {
		Configuration configuration = new Configuration().configure();
		SessionFactory sessionFactory = configuration.buildSessionFactory();
		return sessionFactory;
	}
	

	public void createDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.persist(database);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	
	public CatalogDatabaseImpl getDatabase(Long dbId) {
		Transaction transObj = null;
		try {
			CatalogDatabaseImpl db = (CatalogDatabaseImpl) sessionObj.load(CatalogDatabaseImpl.class, dbId);
			Hibernate.initialize(db);
			return db;
		} catch (HibernateException exObj) {
			exObj.printStackTrace(); 
		} 
		return null;
	}
	
	public void dropDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.delete(database);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	/*
	public void createTable(CatalogTableImpl table) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.persist(table);
			transObj.commit();
			sessionObj.flush();

		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	public void dropTable(CatalogTableImpl table) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.delete(table);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		}
	}
*/
	public CatalogDatabaseImpl getDatabase(String dbName) {
		
		Transaction transObj = null;
		try {
			
			Criteria criteria = sessionObj.createCriteria(CatalogDatabaseImpl.class);
			CatalogDatabaseImpl db  = (CatalogDatabaseImpl) criteria.add(Restrictions.eq("name", dbName))
			                             .uniqueResult();
	
			Hibernate.initialize(db);
			return db;
		} catch (HibernateException exObj) {
			exObj.printStackTrace(); 
		} 
		return null;
	}

	//ironically right now these are teh same
	public void updateDatabase(CatalogDatabaseImpl db) {
		createDatabase(db);
	}
	
}
