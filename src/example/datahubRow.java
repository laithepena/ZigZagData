package example;

import java.io.Serializable;

import scala.collection.Seq;

public class datahubRow implements Serializable {
	
	String local_date_id;
	Long met;
	public String getLocal_date_id() {
		return local_date_id;
	}
	public void setLocal_date_id(String seq) {
		this.local_date_id = seq;
	}
	public Long getMet() {
		return met;
	}
	public void setMet(long l) {
		this.met = l;
	}

}
