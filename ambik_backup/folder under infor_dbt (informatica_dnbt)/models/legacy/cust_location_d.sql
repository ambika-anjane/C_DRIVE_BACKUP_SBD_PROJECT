select	
	
	TO_CHAR(SQ_CUST_LOCS.CUST_ACCOUNT_ID)	   C1_CUSTOMER_ID,
	SQ_CUST_LOCS.ADDRESS1	   C2_ST_ADDRESS1,
	SQ_CUST_LOCS.ADDRESS2	   C3_ST_ADDRESS2,
	SQ_CUST_LOCS.ADDRESS3	   C4_ST_ADDRESS3,
	SQ_CUST_LOCS.ADDRESS4	   C5_ST_ADDRESS4,
	COALESCE
    ((CASE WHEN RTRIM(TRIM(SQ_CUST_LOCS.POSTAL_CODE)) IS NULL THEN NULL
     ELSE TRIM(SQ_CUST_LOCS.POSTAL_CODE)
    END ),'#BIAPPS.ETL_UNSPEC_STR')	   C6_POSTAL_CODE,
	COALESCE
    ((CASE WHEN RTRIM(TRIM(SQ_CUST_LOCS.CITY)) IS NULL THEN NULL
     ELSE TRIM(SQ_CUST_LOCS.CITY)
    END ),'#BIAPPS.ETL_UNSPEC_STR')
	C7_CITY,
	COALESCE
    ((CASE WHEN RTRIM(TRIM(SQ_CUST_LOCS.COUNTY)) IS NULL THEN NULL
     ELSE TRIM(SQ_CUST_LOCS.COUNTY)
    END ),'#BIAPPS.ETL_UNSPEC_STR')
    C8_COUNTY,
	COALESCE
    ((CASE WHEN RTRIM(TRIM(SQ_CUST_LOCS.STATE)) IS NULL THEN NULL
     ELSE TRIM(SQ_CUST_LOCS.STATE)
    END ),'#BIAPPS.ETL_UNSPEC_STR')	   C9_STATE_PROV_CODE,
	COALESCE
    ((CASE WHEN RTRIM(TRIM(SQ_CUST_LOCS.COUNTRY)) IS NULL THEN NULL
    ELSE TRIM(SQ_CUST_LOCS.COUNTRY)
    END ),'#BIAPPS.ETL_UNSPEC_STR')	   C10_COUNTRY_CODE,
	SQ_CUST_LOCS.PHONE	   C11_PHONE_NUM,
	SQ_CUST_LOCS.FAX	   C12_FAX_NUM,
	SQ_CUST_LOCS.EMAIL	   C13_EMAIL_ADDRESS,
	SQ_CUST_LOCS.URL	   C14_WEB_ADDRESS,
	(CASE WHEN SQ_CUST_LOCS.STATUS = 'I' THEN 'N'
    ELSE 'Y'
    END ) C15_ACTIVE_FLG,
	TO_CHAR(SQ_CUST_LOCS.CREATED_BY)	   C16_CREATED_BY_ID,
	TO_CHAR(SQ_CUST_LOCS.LAST_UPDATED_BY)	   C17_CHANGED_BY_ID,
	SQ_CUST_LOCS.CREATION_DATE	   C18_CREATED_ON_DT,
	SQ_CUST_LOCS.LAST_UPDATE_DATE	   C19_CHANGED_ON_DT,
	SQ_CUST_LOCS.LAST_UPDATE_DATE1	   C20_AUX1_CHANGED_ON_DT,
	SQ_CUST_LOCS.LAST_UPDATE_DATE2	   C21_AUX2_CHANGED_ON_DT,
	SQ_CUST_LOCS.DELETE_FLG	   C22_DELETE_FLG,
	TO_CHAR(SQ_CUST_LOCS.CUST_ACCT_SITE_ID)	   C23_INTEGRATION_ID,
	SQ_CUST_LOCS.X_CUSTOM	   C24_X_CUSTOM,
	SQ_CUST_LOCS.ADDRESSEE	   C25_X_ADDRESSEE,
	SQ_CUST_LOCS.CITY	   C26_CITY,
	SQ_CUST_LOCS.COUNTRY	   C27_COUNTRY,
	SQ_CUST_LOCS.STATE	   C28_STATE,
	SQ_CUST_LOCS.COUNTY	   C29_COUNTY
from	
( /* Subselect from SDE_ORA_CustomerLocationDimension.W_CUSTOMER_LOC_DS_SQ_CUST_LOCS
*/


select 
 HZ_LOCATIONS.LAST_UPDATE_DATE
 LAST_UPDATE_DATE,
	HZ_LOCATIONS.LAST_UPDATED_BY LAST_UPDATED_BY,
	HZ_LOCATIONS.CREATION_DATE CREATION_DATE,
	HZ_LOCATIONS.CREATED_BY CREATED_BY,
	HZ_LOCATIONS.ADDRESS1 ADDRESS1,
	HZ_LOCATIONS.COUNTRY COUNTRY,
	HZ_LOCATIONS.CITY CITY,
	HZ_LOCATIONS.POSTAL_CODE POSTAL_CODE,
	HZ_LOCATIONS.STATE STATE,
	HZ_LOCATIONS.COUNTY COUNTY,
	HZ_CUST_ACCT_SITES_ALL.CUST_ACCOUNT_ID CUST_ACCOUNT_ID,
	HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID CUST_ACCT_SITE_ID,
	HZ_CUST_ACCT_SITES_ALL.STATUS STATUS,
	HZ_CUST_ACCT_SITES_ALL.LAST_UPDATE_DATE LAST_UPDATE_DATE1,
	HZ_PARTY_SITES.LAST_UPDATE_DATE LAST_UPDATE_DATE2,
	HZ_LOCATIONS.ADDRESS2 ADDRESS2,
	HZ_LOCATIONS.ADDRESS3 ADDRESS3,
	HZ_LOCATIONS.ADDRESS4 ADDRESS4,
	HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID PARTY_SITE_ID,
	
'N'
 DELETE_FLG,
	'0' X_CUSTOM,
	LKP_EMAIL.EMAIL EMAIL,
	LKP_URL.URL URL,
	LKP_FAX.FAX FAX,
	LKP_PHONE.PHONE PHONE,
	HZ_PARTY_SITES.ADDRESSEE ADDRESSEE
from	((((APPS.HZ_CUST_ACCT_SITES_ALL    HZ_CUST_ACCT_SITES_ALL INNER JOIN (APPS.HZ_PARTY_SITES    HZ_PARTY_SITES INNER JOIN APPS.HZ_LOCATIONS    HZ_LOCATIONS ON HZ_PARTY_SITES.LOCATION_ID=HZ_LOCATIONS.LOCATION_ID) ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID=HZ_PARTY_SITES.PARTY_SITE_ID) LEFT OUTER JOIN 
( /* Subselect from SDE_ORA_CustomerLocationDimension.W_CUSTOMER_LOC_DS_EMAIL
*/


select 
	  

	   
	   HZ_CONTACT_POINTS.OWNER_TABLE_ID OWNER_TABLE_ID,
	SUBSTR(HZ_CONTACT_POINTS.EMAIL_ADDRESS, 0,255) EMAIL
from	APPS.HZ_CONTACT_POINTS   HZ_CONTACT_POINTS
where	(1=1)

And (

(HZ_CONTACT_POINTS.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS'))

)
 And (HZ_CONTACT_POINTS.OWNER_TABLE_NAME = 'HZ_PARTY_SITES'  
AND HZ_CONTACT_POINTS.CONTACT_POINT_TYPE = 'EMAIL' 
AND HZ_CONTACT_POINTS.PRIMARY_FLAG = 'Y'  
AND HZ_CONTACT_POINTS.STATUS = 'A')







)    LKP_EMAIL ON HZ_PARTY_SITES.PARTY_SITE_ID=LKP_EMAIL.OWNER_TABLE_ID) LEFT OUTER JOIN 
( /* Subselect from SDE_ORA_CustomerLocationDimension.W_CUSTOMER_LOC_DS_FAX
*/


select 
	  

	   
	   HZ_CONTACT_POINTS.OWNER_TABLE_ID OWNER_TABLE_ID,
	MAX(SUBSTR(HZ_CONTACT_POINTS.RAW_PHONE_NUMBER, 1,30)) FAX
from	APPS.HZ_CONTACT_POINTS   HZ_CONTACT_POINTS
where	(1=1)

And (

(HZ_CONTACT_POINTS.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS'))

)
 And (HZ_CONTACT_POINTS.OWNER_TABLE_NAME = 'HZ_PARTY_SITES'  
AND HZ_CONTACT_POINTS.CONTACT_POINT_TYPE = 'FAX' 
AND HZ_CONTACT_POINTS.PHONE_LINE_TYPE = 'GEN'  
AND HZ_CONTACT_POINTS.STATUS = 'A')


Group By HZ_CONTACT_POINTS.OWNER_TABLE_ID





)    LKP_FAX ON HZ_PARTY_SITES.PARTY_SITE_ID=LKP_FAX.OWNER_TABLE_ID) LEFT OUTER JOIN 
( /* Subselect from SDE_ORA_CustomerLocationDimension.W_CUSTOMER_LOC_DS_URL
*/


select 
	  

	   
	   HZ_CONTACT_POINTS.OWNER_TABLE_ID OWNER_TABLE_ID,
	SUBSTR(HZ_CONTACT_POINTS.URL, 1,255) URL
from	APPS.HZ_CONTACT_POINTS   HZ_CONTACT_POINTS
where	(1=1)

And (

(HZ_CONTACT_POINTS.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS'))

)
 And (HZ_CONTACT_POINTS.OWNER_TABLE_NAME = 'HZ_PARTY_SITES'  
AND HZ_CONTACT_POINTS.CONTACT_POINT_TYPE = 'WEB' 
AND HZ_CONTACT_POINTS.PRIMARY_FLAG = 'Y'  
AND HZ_CONTACT_POINTS.STATUS = 'A')







)    LKP_URL ON HZ_PARTY_SITES.PARTY_SITE_ID=LKP_URL.OWNER_TABLE_ID) LEFT OUTER JOIN 
( /* Subselect from SDE_ORA_CustomerLocationDimension.W_CUSTOMER_LOC_DS_PHONE
*/


select 
	  

	   
	   HZ_CONTACT_POINTS.OWNER_TABLE_ID OWNER_TABLE_ID,
	MAX(SUBSTR(HZ_CONTACT_POINTS.RAW_PHONE_NUMBER, 1,30)) PHONE
from	APPS.HZ_CONTACT_POINTS   HZ_CONTACT_POINTS
where	(1=1)

And (

(HZ_CONTACT_POINTS.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS'))

)
 And (HZ_CONTACT_POINTS.OWNER_TABLE_NAME = 'HZ_PARTY_SITES'  
AND HZ_CONTACT_POINTS.CONTACT_POINT_TYPE = 'PHONE' 
AND HZ_CONTACT_POINTS.PHONE_LINE_TYPE = 'GEN'  
AND HZ_CONTACT_POINTS.STATUS = 'A')


Group By HZ_CONTACT_POINTS.OWNER_TABLE_ID





)    LKP_PHONE ON HZ_PARTY_SITES.PARTY_SITE_ID=LKP_PHONE.OWNER_TABLE_ID
where	(1=1)

And (


((EXISTS (SELECT 1 FROM
(SELECT HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID
      FROM 
     APPS.HZ_CUST_ACCT_SITES_ALL

      WHERE HZ_CUST_ACCT_SITES_ALL.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS') 
      UNION
      SELECT HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID
      FROM 
     APPS.HZ_CUST_ACCT_SITES_ALL
 INNER JOIN
        
     APPS.HZ_PARTY_SITES

      ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID=HZ_PARTY_SITES.PARTY_SITE_ID
      WHERE HZ_PARTY_SITES.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS') 
      UNION
      SELECT HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID
      FROM (
     APPS.HZ_CUST_ACCT_SITES_ALL

  INNER JOIN (
     APPS.HZ_PARTY_SITES
 INNER JOIN 
     APPS.HZ_LOCATIONS

  ON HZ_PARTY_SITES.LOCATION_ID          =HZ_LOCATIONS.LOCATION_ID)
  ON HZ_CUST_ACCT_SITES_ALL.PARTY_SITE_ID=HZ_PARTY_SITES.PARTY_SITE_ID)
      WHERE HZ_LOCATIONS.LAST_UPDATE_DATE > TO_DATE(SUBSTR('#BIAPPS.LAST_EXTRACT_DATE',0,19),'YYYY-MM-DD HH24:MI:SS') 
	  ) TEMP 
WHERE  HZ_CUST_ACCT_SITES_ALL.CUST_ACCT_SITE_ID=TEMP.CUST_ACCT_SITE_ID )))


)







)   SQ_CUST_LOCS
where	(1=1)







