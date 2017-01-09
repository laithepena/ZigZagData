package audit;

import java.io.Serializable;

public class FeedCols implements Serializable{
	
	String PSN_LOGTIME_PST;
	String PSN_ASSETID_Cr;
	String PSN_DURATION;
	String PSN_EVENT;
	String PSN_EVENTTIME_EST;
	double PSN_NPT;
	double PSN_SCALE;
	String Last_Updated_pst;
	
	public String getPSN_LOGTIME_PST() {
		return PSN_LOGTIME_PST;
	}
	public void setPSN_LOGTIME_PST(String pSN_LOGTIME_PST) {
		PSN_LOGTIME_PST = pSN_LOGTIME_PST;
	}
	public String getPSN_ASSETID_Cr() {
		return PSN_ASSETID_Cr;
	}
	public void setPSN_ASSETID_Cr(String pSN_ASSETID_Cr) {
		PSN_ASSETID_Cr = pSN_ASSETID_Cr;
	}
	public String getPSN_DURATION() {
		return PSN_DURATION;
	}
	public void setPSN_DURATION(String pSN_DURATION) {
		PSN_DURATION = pSN_DURATION;
	}
	public String getPSN_EVENT() {
		return PSN_EVENT;
	}
	public void setPSN_EVENT(String pSN_EVENT) {
		PSN_EVENT = pSN_EVENT;
	}
	public String getPSN_EVENTTIME_EST() {
		return PSN_EVENTTIME_EST;
	}
	public void setPSN_EVENTTIME_EST(String pSN_EVENTTIME_EST) {
		PSN_EVENTTIME_EST = pSN_EVENTTIME_EST;
	}
	public double getPSN_NPT() {
		return PSN_NPT;
	}
	public void setPSN_NPT(double pSN_NPT) {
		PSN_NPT = pSN_NPT;
	}
	public double getPSN_SCALE() {
		return PSN_SCALE;
	}
	public void setPSN_SCALE(double pSN_SCALE) {
		PSN_SCALE = pSN_SCALE;
	}
	public String getLast_Updated_pst() {
		return Last_Updated_pst;
	}
	public void setLast_Updated_pst(String last_Updated_pst) {
		Last_Updated_pst = last_Updated_pst;
	}
	
	

}
