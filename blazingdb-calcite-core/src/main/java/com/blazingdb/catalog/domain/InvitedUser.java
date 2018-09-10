package com.blazingdb.catalog.domain;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_EMPTY)
@Entity
@Table(name = "blazing_database")public class InvitedUser  implements java.io.Serializable {

	


	/**
	 * 
	 */
	private static final long serialVersionUID = -3564977177806071848L;

	@Id
	@GeneratedValue
	@Column(name="id")	
	private Long id;
	
	@Column
	private String email;
	
	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "database_id")
	private Database database;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Database getDatabase() {
		return database;
	}

	public void setDatabase(Database database) {
		this.database = database;
	}
	
	

   
}
