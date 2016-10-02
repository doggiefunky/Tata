data temp;
	set "C:\SAS\Classwork\example\hsmaster";
run;

libname eloca "C:\SAS\Classwork\example\";

data temp;
	set eloca.hsmaster;
run;

data temp;
	set eloca.hsmaster;
	where (female = 0);
run;

proc sql;
   delete from temp
      where (race = 3);
quit;



libname mloca "C:\SAS\classwork\out\";
libname an "C:\SAS\Classwork\AN\";
libname cohort "C:\SAS\Classwork\HV cohort\";
libname rev "C:\SAS\Classwork\HV cohort\rev\";
/*WRONG WAY*/
data hv;
	set 
	an.hv1997
	an.hv1998
	an.hv1999
	an.hv2000
	an.hv2001
	an.hv2002
	an.hv2003
	an.hv2004
	an.hv2005
	an.hv2006
	an.hv2007
	an.hv2008
	an.hv2009
	an.hv2010
	an.hv2011
	an.hv2012
	an.hv2013
	;
run;
/*COLLECT SAS HV DATA ALL MALIGNANT NEOPLASMS*/
data hv_part_1(keep = ID DISE_CODE ID_BIRTHDAY ID_SEX APPL_DATE DEATH_MARK DEATH_DATE HOSP_ID);
	set 
	an.hv1997
	an.hv1998
	an.hv1999
	an.hv2000
	an.hv2001
	an.hv2002
	an.hv2003
	an.hv2004
	;
	where ((DISE_CODE like "1___")or(DISE_CODE like "20__")or(DISE_CODE like "21__")or(DISE_CODE like "23__"));
	if STOP_REASON = "M" then DEATH_MARK = "Y";
	rename STOP_DATE=DEATH_DATE;
run;
data hv_part_2(keep = ID DISE_CODE ID_BIRTHDAY ID_SEX APPL_DATE DEATH_MARK DEATH_DATE HOSP_ID);
	set
	an.hv2005
	an.hv2006
	an.hv2007
	an.hv2008
	an.hv2009
	;
	where ((DISE_CODE like "1___")or(DISE_CODE like "20__")or(DISE_CODE like "21__")or(DISE_CODE like "23__"));
run;
data hv_part_3(keep = ID DISE_CODE ID_BIRTHDAY ID_SEX APPL_DATE DEATH_MARK DEATH_DATE HOSP_ID);
	set
	an.hv2010
	an.hv2011
	an.hv2012
	an.hv2013
	;
	where ((ICD9CM_CODE like "1___")or(ICD9CM_CODE like "20__")or(ICD9CM_CODE like "21__")or(ICD9CM_CODE like "23__"));
	rename ICD9CM_CODE=DISE_CODE;
	rename BIRTHDAY=ID_BIRTHDAY;
	rename SEX=ID_SEX;
run;
/*CHECK DATA*/
proc freq data = hv_part_1;
	table ID_SEX;
run;
proc freq data = hv_part_2;
	table ID_SEX;
run;
proc freq data = hv_part_3;
	table ID_SEX;
run;
data test;
	set hv_part_1;
	where (ID_SEX = "0");
run;
/*ERROR 1*/
data test;
	set hv_part_1;
	where (HOSP_ID="2d47a89a10c4289736333429aef81b7fXX"and(ID_SEX~="M")and(ID_SEX~="F"));
run;
proc freq data = test;
	table ID_SEX;
run;
data hv_part_1;
	set hv_part_1;
	if ((HOSP_ID="2d47a89a10c4289736333429aef81b7fXX")and(ID_SEX="0")) then delete;
run;
/*ERROR 2*/
data test;
	set hv_part_1;
	where ((HOSP_ID = "52e667399b1db23c4a68f4ccce9b64ee13")and(ID_SEX~="M")and(ID_SEX~="F"));
run;
proc freq data = test;
	table ID_SEX;
run;
data hv_part_1;
	set hv_part_1;
	if ((HOSP_ID="52e667399b1db23c4a68f4ccce9b64ee13")and(ID_SEX="0")) then delete;
	if ((HOSP_ID="52e667399b1db23c4a68f4ccce9b64ee13")and(ID_SEX="1")) then delete;
	if ((HOSP_ID="52e667399b1db23c4a68f4ccce9b64ee13")and(ID_SEX="2")) then delete;
run;
/*ERROR 3*/
data test;
	set hv_part_1;
	where ((HOSP_ID = "8caa684d0f1d7e274c018b4cc66bae4915")and(ID_SEX~="M")and(ID_SEX~="F"));
run;
data hv_part_1;
	set hv_part_1;
	if ((HOSP_ID="8caa684d0f1d7e274c018b4cc66bae4915")and(ID_SEX="0")) then delete;
run;
/*COMBINE DATA*/
data mloca.hv(drop = hosp_id);
	set hv_part_1 hv_part_2 hv_part_3;
run;
/*HOUSEKEEPING*/
proc datasets library=work;
   delete hv_part_1 hv_part_2 hv_part_3;
run;
/*SORT AND GET FIRST OCCURANCE*/
PROC SORT DATA=mloca.hv OUT=mloca.hv;
  BY  ID;
RUN;
PROC SORT DATA=mloca.hv OUT=mloca.hv;
  BY  ID APPL_DATE;
RUN;
data mloca.hv;
	set mloca.hv;
	by ID;
	if first.ID;
run;
data mloca.hv;
	set mloca.hv;
	rename APPL_DATE = APPL_DATE_HV;
run;

/*match hv with cd*/
proc sql;
	create table mloca.hvcd1997 as
	select *
	from mloca.hv, cohort.cd1997(keep = ID FEE_YM HOSP_ID APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO FUNC_DATE ACODE_ICD9_1 ACODE_ICD9_2 ACODE_ICD9_3)
	where (hv.ID = cd1997.ID)
	order by hv.ID;
quit;
/*macro*/
%macro hcvd(visitName);
proc sql;
	create table temp as
	select *
	from mloca.hv, cohort.&visitName.(keep = ID FEE_YM HOSP_ID APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO FUNC_DATE ACODE_ICD9_1 ACODE_ICD9_2 ACODE_ICD9_3)
	where (hv.ID = &visitName..ID)
	order by hv.ID;
quit;
proc append base = mloca.hvcd data = temp /*FORCE*/; 
run;
%mend hcvd;
%hcvd(cd1997);
%hcvd(cd1998);
%hcvd(cd1999);
%hcvd(cd2000);
%hcvd(cd2001);
%hcvd(cd2002);
%hcvd(cd2003);
%hcvd(cd2004);
%hcvd(cd2005);
%hcvd(cd2006);
%hcvd(cd2007);
%hcvd(cd2008);
%hcvd(cd2009);
%hcvd(cd2010);
%hcvd(cd2011);
%hcvd(cd2012);
%hcvd(cd2013);
/*FIX DD*/
%macro datafixDD(fname);
data rev.&fname.;
	length CASE_TYPE $ 2;
	length ICD9CM_CODE ICD9CM_CODE_1 ICD9CM_CODE_2 ICD9CM_CODE_3 ICD9CM_CODE_4 $ 15;
	set cohort.&fname.;
	format CASE_TYPE $ 2.;
	format ICD9CM_CODE ICD9CM_CODE_1 ICD9CM_CODE_2 ICD9CM_CODE_3 ICD9CM_CODE_4 $ 15.;
run;
data rev.&fname.;
	retain 
	FEE_YM APPL_TYPE HOSP_ID APPL_DATE CASE_TYPE SEQ_NO ID ID_BIRTHDAY GAVE_KIND TRAC_EVEN CARD_SEQ_NO FUNC_TYPE 
	IN_DATE OUT_DATE APPL_BEG_DATE APPL_END_DATE E_BED_DAY S_BED_DAY PRSN_ID DRG_CODE 
	EXT_CODE_1 EXT_CODE_2 TRAN_CODE 
	ICD9CM_CODE ICD9CM_CODE_1 ICD9CM_CODE_2 ICD9CM_CODE_3 ICD9CM_CODE_4 
	ICD_OP_CODE ICD_OP_CODE_1 ICD_OP_CODE_2 ICD_OP_CODE_3 ICD_OP_CODE_4 
	DIAG_AMT ROOM_AMT MEAL_AMT AMIN_AMT RADO_AMT THRP_AMT SGRY_AMT PHSC_AMT HD_AMT BLOD_AMT ANE_AMT METR_AMT DRUG_AMT DSVC_AMT NRTP_AMT INJT_AMT BABY_AMT CHARG_AMT MED_AMT PART_AMT APPL_AMT 
	EB_APPL30_AMT EB_PART30_AMT EB_APPL60_AMT EB_PART60_AMT EB_APPL61_AMT EB_PART61_AMT SB_APPL30_AMT SB_PART30_AMT SB_APPL90_AMT SB_PART90_AMT SB_APPL180_AMT SB_PART180_AMT SB_APPL181_AMT SB_PART181_AMT 
	PART_MARK ID_SEX  ;
	set rev.&fname.;
run;
%mend datafixDD;
%datafixDD(dd1997);
%datafixDD(dd1998);
%datafixDD(dd1999);
%datafixDD(dd2000);
%datafixDD(dd2001);
%datafixDD(dd2002);
%datafixDD(dd2003);
%datafixDD(dd2004);
%datafixDD(dd2005);
%datafixDD(dd2006);
%datafixDD(dd2007);
%datafixDD(dd2008);
%datafixDD(dd2009);
%datafixDD(dd2010);
%datafixDD(dd2011);
%datafixDD(dd2012);
%datafixDD(dd2013);
/* match hv dd*/
%macro hvdd(visitName);
proc sql;
	create table temp as
	select *
	from mloca.hv, rev.&visitName.(keep = ID FEE_YM HOSP_ID APPL_TYPE APPL_DATE CASE_TYPE SEQ_NO IN_DATE OUT_DATE ICD9CM_CODE ICD9CM_CODE_1 ICD9CM_CODE_2 ICD9CM_CODE_3 ICD9CM_CODE_4)
	where (hv.ID = &visitName..ID)
	order by hv.ID;
quit;
proc append base = mloca.hvdd data = temp /*FORCE*/; 
run;
%mend hvdd;
%hvdd(dd1997);
%hvdd(dd1998);
%hvdd(dd1999);
%hvdd(dd2000);
%hvdd(dd2001);
%hvdd(dd2002);
%hvdd(dd2003);
%hvdd(dd2004);
%hvdd(dd2005);
%hvdd(dd2006);
%hvdd(dd2007);
%hvdd(dd2008);
%hvdd(dd2009);
%hvdd(dd2010);
%hvdd(dd2011);
%hvdd(dd2012);
%hvdd(dd2013);
trctgygy
