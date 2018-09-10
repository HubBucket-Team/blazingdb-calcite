package com.blazingdb.catalog.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(include=JsonSerialize.Inclusion.NON_EMPTY)
@Entity
@Table(name = "user_token")
public class UserToken  implements java.io.Serializable, Comparable<UserToken>  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1960285892320923404L;


	@Id
	@GeneratedValue
	@Column(name="id")	
	private Long id;
	
	
	@Column(name="username")
	private String username;
	
	@Column(name = "token")
	private String token;

	
	public String getToken(){
		return token;
	}
	
	public void setToken(String token){
		this.token = token;
	}
	
	public Long getId() {
		return id;
	}


	public void setId(Long id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}


	@Override
	public int compareTo(UserToken o) {
		if(o.getId() > this.getId()){
			return 1;
		}else if(o.getId() < this.getId()){
			return -1;
		}else{
			return 0;
		}
		
	}
	
	
   
}
