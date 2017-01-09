package audit;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class AdrCols  implements Serializable{

	/**
	 * 
	 */
/*	Adrcols(){	
		 Properties colProp = new Properties();
		 FileInputStream input = null;
	try {
		input = new FileInputStream("resources/columnConfig.properties");
		colProp.load(input);
	} catch (FileNotFoundException e1) {
		e1.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
	
}*/
	private static final long serialVersionUID = 1L;
	String RTASSETID;
	String SUBSCRIBERREF;
	String MSGTRCKID;
	String EVENTTIME_EST;
	String LOGTIME_PST;
	String POISAVAILTRACKING;
	String NETWORK;
	String DISTRIBUTOR;
	String CLID;
	String ASSETID_Cr;
	String DATABASEID_Cr;
	String NAME_Cr;
	String BALIBID;
	String ACCOUNTID;
	String ACCOUNT;
	String INVENTORYSRCACCOUNT;
	String INVENTORYSRCENABLED;
	String ADCOUNT;
	String ADMAPID;
	String POISMSGTRACKING;
	String PRMSGREF;
	String TYPE;
	String PLACEMENTID;
	String PROGRAMPOSITION;
	String ADPOSITION;
	String ADMAPADUNITID;
	String ADUNITID;
	String ADSIDENTITY;
	String ACTION;
	String RTPROVIDERID;
	String ADMID;
	String LOG_FILE_NAME;
	String Last_Updated_pst;
	
	public String getRTASSETID() {
		return RTASSETID;
	}
	public void setRTASSETID(String rTASSETID) {
		RTASSETID = rTASSETID;
	}
	public String getSUBSCRIBERREF() {
		return SUBSCRIBERREF;
	}
	public void setSUBSCRIBERREF(String sUBSCRIBERREF) {
		SUBSCRIBERREF = sUBSCRIBERREF;
	}
	public String getMSGTRCKID() {
		return MSGTRCKID;
	}
	public void setMSGTRCKID(String mSGTRCKID) {
		MSGTRCKID = mSGTRCKID;
	}
	public String getEVENTTIME_EST() {
		return EVENTTIME_EST;
	}
	public void setEVENTTIME_EST(String eVENTTIME_EST) {
		EVENTTIME_EST = eVENTTIME_EST;
	}
	public String getLOGTIME_PST() {
		return LOGTIME_PST;
	}
	public void setLOGTIME_PST(String lOGTIME_PST) {
		LOGTIME_PST = lOGTIME_PST;
	}
	public String getPOISAVAILTRACKING() {
		return POISAVAILTRACKING;
	}
	public void setPOISAVAILTRACKING(String pOISAVAILTRACKING) {
		POISAVAILTRACKING = pOISAVAILTRACKING;
	}
	public String getNETWORK() {
		return NETWORK;
	}
	public void setNETWORK(String nETWORK) {
		NETWORK = nETWORK;
	}
	public String getDISTRIBUTOR() {
		return DISTRIBUTOR;
	}
	public void setDISTRIBUTOR(String dISTRIBUTOR) {
		DISTRIBUTOR = dISTRIBUTOR;
	}
	public String getCLID() {
		return CLID;
	}
	public void setCLID(String cLID) {
		CLID = cLID;
	}
	public String getASSETID_Cr() {
		return ASSETID_Cr;
	}
	public void setASSETID_Cr(String aSSETID_Cr) {
		ASSETID_Cr = aSSETID_Cr;
	}
	public String getDATABASEID_Cr() {
		return DATABASEID_Cr;
	}
	public void setDATABASEID_Cr(String dATABASEID_Cr) {
		DATABASEID_Cr = dATABASEID_Cr;
	}
	public String getNAME_Cr() {
		return NAME_Cr;
	}
	public void setNAME_Cr(String nAME_Cr) {
		NAME_Cr = nAME_Cr;
	}
	public String getBALIBID() {
		return BALIBID;
	}
	public void setBALIBID(String bALIBID) {
		BALIBID = bALIBID;
	}
	public String getACCOUNTID() {
		return ACCOUNTID;
	}
	public void setACCOUNTID(String aCCOUNTID) {
		ACCOUNTID = aCCOUNTID;
	}
	public String getACCOUNT() {
		return ACCOUNT;
	}
	public void setACCOUNT(String aCCOUNT) {
		ACCOUNT = aCCOUNT;
	}
	public String getINVENTORYSRCACCOUNT() {
		return INVENTORYSRCACCOUNT;
	}
	public void setINVENTORYSRCACCOUNT(String iNVENTORYSRCACCOUNT) {
		INVENTORYSRCACCOUNT = iNVENTORYSRCACCOUNT;
	}
	public String getINVENTORYSRCENABLED() {
		return INVENTORYSRCENABLED;
	}
	public void setINVENTORYSRCENABLED(String iNVENTORYSRCENABLED) {
		INVENTORYSRCENABLED = iNVENTORYSRCENABLED;
	}
	public String getADCOUNT() {
		return ADCOUNT;
	}
	public void setADCOUNT(String aDCOUNT) {
		ADCOUNT = aDCOUNT;
	}
	public String getADMAPID() {
		return ADMAPID;
	}
	public void setADMAPID(String aDMAPID) {
		ADMAPID = aDMAPID;
	}
	public String getPOISMSGTRACKING() {
		return POISMSGTRACKING;
	}
	public void setPOISMSGTRACKING(String pOISMSGTRACKING) {
		POISMSGTRACKING = pOISMSGTRACKING;
	}
	public String getPRMSGREF() {
		return PRMSGREF;
	}
	public void setPRMSGREF(String pRMSGREF) {
		PRMSGREF = pRMSGREF;
	}
	public String getTYPE() {
		return TYPE;
	}
	public void setTYPE(String tYPE) {
		TYPE = tYPE;
	}
	public String getPLACEMENTID() {
		return PLACEMENTID;
	}
	public void setPLACEMENTID(String pLACEMENTID) {
		PLACEMENTID = pLACEMENTID;
	}
	public String getPROGRAMPOSITION() {
		return PROGRAMPOSITION;
	}
	public void setPROGRAMPOSITION(String pROGRAMPOSITION) {
		PROGRAMPOSITION = pROGRAMPOSITION;
	}
	public String getADPOSITION() {
		return ADPOSITION;
	}
	public void setADPOSITION(String aDPOSITION) {
		ADPOSITION = aDPOSITION;
	}
	public String getADMAPADUNITID() {
		return ADMAPADUNITID;
	}
	public void setADMAPADUNITID(String aDMAPADUNITID) {
		ADMAPADUNITID = aDMAPADUNITID;
	}
	public String getADUNITID() {
		return ADUNITID;
	}
	public void setADUNITID(String aDUNITID) {
		ADUNITID = aDUNITID;
	}
	public String getADSIDENTITY() {
		return ADSIDENTITY;
	}
	public void setADSIDENTITY(String aDSIDENTITY) {
		ADSIDENTITY = aDSIDENTITY;
	}
	public String getACTION() {
		return ACTION;
	}
	public void setACTION(String aCTION) {
		ACTION = aCTION;
	}
	public String getRTPROVIDERID() {
		return RTPROVIDERID;
	}
	public void setRTPROVIDERID(String rTPROVIDERID) {
		RTPROVIDERID = rTPROVIDERID;
	}
	public String getADMID() {
		return ADMID;
	}
	public void setADMID(String aDMID) {
		ADMID = aDMID;
	}
	public String getLOG_FILE_NAME() {
		return LOG_FILE_NAME;
	}
	public void setLOG_FILE_NAME(String lOG_FILE_NAME) {
		LOG_FILE_NAME = lOG_FILE_NAME;
	}
	public String getLast_Updated_pst() {
		return Last_Updated_pst;
	}
	public void setLast_Updated_pst(String last_Updated_pst) {
		Last_Updated_pst = last_Updated_pst;
	}


}
