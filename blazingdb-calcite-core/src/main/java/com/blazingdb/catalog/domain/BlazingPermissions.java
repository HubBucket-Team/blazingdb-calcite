package com.blazingdb.catalog.domain;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;


@Entity
@Table(name = "blazing_permission")
public class BlazingPermissions implements java.io.Serializable {

	public BlazingPermissions(){
		
	}
	
	public BlazingPermissions(String databaseName, String tableName,String columnName, String permissionType){
		this.tableName = tableName;
		this.columnName = columnName;
		this.permissionType = permissionType;
		this.databaseName = databaseName;
	}
	public BlazingPermissions(String username, String roleName, String databaseName, String tableName,String columnName, String permissionType){
		this.tableName = tableName;
		this.columnName = columnName;
		this.permissionType = permissionType;
		this.databaseName = databaseName;
		this.username = username;
		this.roleName = roleName;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1986496765408742899L;

	@Id
	@GeneratedValue
	@Column(name="id")	
	private Long id;
	
	@Column(name = "username",  nullable = true)
	private String username;

	@Column(name = "role_name",  nullable = true)
	private String roleName;
	
	@Column(name = "database_name",  nullable = false)
	private String databaseName;
	
	@Column(name = "table_name",  nullable = false)
	private String tableName;
	
	@Column(name = "column_name",  nullable = false)
	private String columnName;
	
	@Column(name = "permission_type",  nullable = false)
	private String permissionType;
	
	
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

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getPermissionType() {
		return permissionType;
	}

	public void setPermissionType(String permissionType) {
		this.permissionType = permissionType;
	}

	/**
	 * 
	 */

	
	
}
