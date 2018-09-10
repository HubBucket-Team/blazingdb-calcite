package com.blazingdb.catalog.domain;


import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;


@JsonSerialize(include=JsonSerialize.Inclusion.NON_EMPTY)
@Entity
@Table(name = "blazing_database")public class Database  implements java.io.Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -344290295409713025L;

	@Id
	@GeneratedValue
	@Column(name="id")	
	private Long id;
	
	@Column
	private String name;
	
	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "user_id")
	private Users user;

	public Long getId() {
		return id;
	}

	public List<Users> getUsers() {
		return users;
	}

	public void setUsers(List<Users> users) {
		this.users = users;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Users getUser() {
		return user;
	}

	public void setUser(Users user) {
		this.user = user;
	}
	
	@Transient
	private boolean owned = false;
	

   public boolean isOwned() {
		return owned;
	}

	public void setOwned(boolean owned) {
		this.owned = owned;
	}

@ManyToMany(cascade =CascadeType.ALL)
   @JoinTable(name="user_database", 
   joinColumns={@JoinColumn(name="database_id")},
   inverseJoinColumns={@JoinColumn(name="user_id")})
   private List<Users> users = new ArrayList<Users>();
   
}
