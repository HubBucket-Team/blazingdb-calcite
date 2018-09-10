package com.blazingdb.catalog.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Obeto de identidad de configuraciones. Esto al momento solo guarda lo que se considera puntaje bueno regular y malo
 * @author felipe
 *
 */
@Entity
@Table(name = "settings")
public class Settings implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1258400268575988169L;

	@Id
	@Column(name="settings_key")	
	private String settingsKey;
	
	@Column
	private String value;

	public String getSettingsKey() {
		return settingsKey;
	}

	public void setKey(String settingsKey) {
		this.settingsKey = settingsKey;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
			
	
}
