package com.blazingdb.catalog.domain;

// Generated Nov 1, 2012 3:48:29 PM by Hibernate Tools 3.4.0.CR1

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * Objeto dominio de las autoridades. Estos se crean al hacer usuarios o cambiar sus permisos.
 * @author felipe
 *
 */
@Entity
@Table(name = "authorities")
public class Authorities implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6481612214353298682L;
	private AuthoritiesId id;
	private Users users;

	public Authorities() {
	}

	public Authorities(AuthoritiesId id, Users users) {
		this.id = id;
		this.users = users;
	}

	@EmbeddedId
	@AttributeOverrides({
			@AttributeOverride(name = "username", column = @Column(name = "username", nullable = false, length = 50)),
			@AttributeOverride(name = "authority", column = @Column(name = "authority", nullable = false, length = 50)),
			@AttributeOverride(name = "enabled", column = @Column(name = "enabled", nullable = false)) })
	public AuthoritiesId getId() {
		return this.id;
	}

	public void setId(AuthoritiesId id) {
		this.id = id;
	}

	@ManyToOne(fetch = FetchType.EAGER)
	@JoinColumn(name = "username",referencedColumnName="username", nullable = false, insertable = false, updatable = false)
	public Users getUsers() {
		return this.users;
	}

	public void setUsers(Users users) {
		this.users = users;
	}

}
