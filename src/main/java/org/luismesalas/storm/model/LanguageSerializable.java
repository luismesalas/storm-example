package org.luismesalas.storm.model;

import java.io.Serializable;

public class LanguageSerializable implements Serializable {

    private static final long serialVersionUID = 3876555573849372008L;

    private String lang;
    private Double prob;

    public LanguageSerializable(String lang, Double prob) {
	super();
	this.lang = lang;
	this.prob = prob;
    }

    public String getLang() {
	return lang;
    }

    public void setLang(String lang) {
	this.lang = lang;
    }

    public Double getProb() {
	return prob;
    }

    public void setProb(Double prob) {
	this.prob = prob;
    }

}
