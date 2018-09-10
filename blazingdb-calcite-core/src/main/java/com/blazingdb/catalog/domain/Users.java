package com.blazingdb.catalog.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Cascade;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Objeto de identidad de los usuarios que tienen acceso al sistema. Se crean en el panel de administrar usuarios por
 * los administradores.
 * 
 * @author felipe
 *
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
@Entity
@Table(name = "users")
public class Users implements java.io.Serializable, Comparable<Users> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4329893350082662825L;
	@Id
	@GeneratedValue
	@Column(name = "id")
	private Long id;

	private String username = "";

	private String password;
	@Column
	private String name;

	@Column
	private boolean showTips;

	public boolean isShowTips() {
		return showTips;
	}

	public void setShowTips(boolean showTips) {
		this.showTips = showTips;
	}

	@ManyToMany(mappedBy = "users")
	private List<Database> databases = new ArrayList<Database>();

	private boolean enabled;
	// @JsonIgnore
	// @OneToMany(fetch = FetchType.EAGER, mappedBy = "users")

	@JsonIgnore
	@OneToMany(fetch = FetchType.EAGER, mappedBy = "users")
	@Cascade({ org.hibernate.annotations.CascadeType.SAVE_UPDATE })
	private Set<Authorities> authoritieses;

	@Transient
	public String getType() {
		if (this.username.equals("")) {
			return "";
		}
		boolean isLeader = false;
		boolean isAdmin = false;

		for (Authorities authority : this.getAuthoritieses()) {
			if (authority.getId().getAuthority().equals("ROLE_ADMIN")) {
				isAdmin = true;
			} else if (authority.getId().getAuthority().equals("ROLE_DOCUMENT")) {
				isLeader = true;
			}
		}

		if (isAdmin) {

			return "admin";
		} else if (isLeader) {
			return "document";
		} else {
			return "user";
		}
	}

	public Users() {
	}

	public Long getId() {
		return id;
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

	@Column(name = "username", unique = true, nullable = false)
	public String getUsername() {
		return this.username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	@JsonIgnore
	@Column(name = "password", nullable = false, length = 50)
	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Column(name = "enabled", nullable = false)
	public boolean isEnabled() {
		return this.enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public Set<Authorities> getAuthoritieses() {
		return this.authoritieses;
	}

	public void setAuthoritieses(Set<Authorities> authoritieses) {
		this.authoritieses = authoritieses;
	}

	@Transient
	@JsonIgnore
	public boolean isAdmin() {
		for (Authorities authority : this.authoritieses) {
			if (authority.getId().getAuthority().equals("ROLE_ADMIN")) {
				return true;
			}
		}
		return false;
	}

	public List<Database> getDatabases() {
		return databases;
	}

	public void setDatabases(List<Database> databases) {
		this.databases = databases;
	}

	@Override
	public int compareTo(Users o) {
		// TODO Auto-generated method stub
		return this.id.compareTo(o.getId());
	};

	@Override
	public boolean equals(Object aThat) {
		Users thatUser = (Users) aThat;
		return thatUser.getId().equals(thatUser.getId());
	}

}
