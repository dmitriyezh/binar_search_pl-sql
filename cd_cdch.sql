create or replace package cd_cdch authid definer is

  -- Author  : DOBROVOLSKY
  -- Created : 2015-04-13 15:21:52
  -- Purpose : ������ � ��������� ����������� ��������, ������������ � ��������� ���� ��������� �������
  
    
  Version  CONSTANT VARCHAR2(200 char) := '$Id. cd_cdch {7.06.03} 13.04.2015/15.04.2021 12:46 CD Vct $';
  -------------------------------------------------------------------------------------------------------
  -- Vct 15.04.2015 - ������ ������ 
  -- Vct 29.04.2015 {6.98.03} � ������ ���������� ����� ������� �������� 'CR_XML' -- Vct 29.04.2015 (z.152775) - ��������� ��� ������� Credit_Registry (�� ����, ����� ��������� ��������������� ��� ����� ������� �����)
  -- Vct 09.02.2016 {6.99.01}  -- z.160830 - ���������� has_register_history
  -- Vct 17.02.2016 {6.99.02}  -- z.161129 - ���������� has_register_history
  -- Vct 02.03.2016 {6.99.03}  -- z.161491  - ���������� has_register_history
  -- Vct 02.08.2016 {6.99.04}  -- ���������� rule_REQUEST16 � ����� ������� ������ ��� ��������� ���������� �������, ������������ ���� �������
  -- Vct 16.08.2016 {6.99.05}  -- ���������� ������ ����� - 668
  -- Vct 01.11.2016 {7.01.01}  -- ci_RULE_REQUEST2 - ��� ������ 800190_30 - ��� ��������� ��������� ��� ��������� ����� �������
  -- Vct 05.12.2016 {7.01.02} z.167590 - ����� ���������� �������� ������
  -- Vct 15.08.2017 {7.02.01} ��������� has_registered_history ��� ���� 8 (800190_30)
  -- Vct 28.06.2018 {7.03.01} ClearRegisteredHistory_svk z.182475 
  -- Vct 25.01.2019 {7.04.01} z.190592 - ��������� �������� ��. ��������� �� ����������� ��� ������������� ����� �������������� ��������� �� �����.
  -- Vct 11.03.2019 {7.04.02} z.191849 has_register_history
  -- Vct 02.10.2019 {7.05.01} Register_all_reports - ����������� �������� PA
  -- Vct 28.10.2019 {7.05.02} z.196306 
  -- Vct 08.11.2019 {7.05.03} 
  -- Vct 31.12.2019 {7.05.04} z.197857 bReplaceData_flag, DelHistory_for_Replace
  -- Vct 23.06.2020 {7.06.01} Register_all_reports, z.204079
  -- Vct 18.12.2020 {7.06.02} - ������������ � ���������� DelAllRegisteredHistory
  -- Vct 15.04.2021 {7.06.03} z.211947, Clear_RegHistory_For_Day, Clear_RegHistory_For_Day_AT
  -------------------------------------------------------------------------------------------------------
  -- 
  -------------------------------------------------------------------------------------------------------
  -- ��� ��� ����� ���� ���������� �������� ������������������ ������, ��� "������������ � ���" 
  SUBTYPE T_CDCH_CODENAME Is Varchar2(50);
  ----
  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� ��������
  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
  -- ��������� ��� ��� ����������� ���� �� �����, �.�. ��� �������������� �� �������������� ��������� � cdh_doc
  cc_PROVISION_Alias  T_CDCH_CODENAME := 'CPROVISION'; -- ��� ����������� �������������
  ----
-- ��������� ������� "���������" ������ ����������� �������� ��� ���
  Procedure ClearCDCH_TMPTBL(pvc_SEssionID In Varchar2);

-- ��������� ������� "���������" ������ ����������� �������� ��� ��� � ��������
  Procedure ClearwCommitCDCH_TMPTBL(pvc_SEssionID In Varchar2);

-- ��������� ������� "���������" ������ ����������� �������� ��� ��� � ���������� ����������
  Procedure ClearAutoTranCDCH_TMPTBL(pvc_SEssionID In Varchar2);
------------------------------------------------------------------
  -- ��������� ����������� ������ 
  --�������� ������ �� ��������� ������ � ��������������� ���������� 
  Procedure Register_all_reports(pvc_sessionID In Varchar2);  
------------------------------------------------------------------
  -- ������� ���������� 1, ���� ���������� ������� ������������������ � ����� ������ �� 
  -- �������� ��� ������� ���, 0 � ��������� ������
  Function session_data_exists(pvc_sessionID in Varchar2) Return Integer;  
------------------------------------------------------------------
  -- ����� �-��� ��������������� ����������� ������ �� ��������, �������������� � ���
  -- ���������� ������� ��� pvcObjectType:
  --  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
  --  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� �������
  --  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
  Function Register_cdch_tempobj(pvcObjectType IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                                    , nObjID In Number -- �������� ������������� ��������������� �������
                                    , pvcSessionID in Varchar2 -- ������������� ������ ������������
                                    , pvcReportType in Varchar2 -- ������� ��� ���� ������
                                    , pnAStatus in Integer DEfault 0 -- ���. ������, (1 - ��������� �� ����������� �������)
                                    , pdEventDate in Date Default CD.Get_LSDATE -- ����, ������� ������������ �����
                                    ) Return Pls_integer;  
----------------------------------------------------------------------
  -- �-��� ���������� ��������� ���� ����������� ������� � ������� ���
  -- ���������� �������� ��� pvcObjectType:
  --  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
  --  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� ��������
  --  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
  Function Get_LastRegisterDate(pvcObjectType in Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                                , pnObjID in NUMBER -- ������������� ������������������� �������
                                , TypeRep in CDCH.ccdchtype%TYPE -- ��� ������ 
                                , evDATE in DATE DEFAULT CD.get_lsdate) RETURN DATE;
----------------------------------------------------------------------
  -- �-��� ��������� 1, ���� ������ ��� ��� ��� ��������������� c ����� ������ ������� ���������,
  -- ������������� ���������� piruleMode
  -- �������� piruleMode: 
  -- 0 - ������ ��������� ������������������, ���� �� ��������������� � ��� = evDATE
  -- 1 - ������ ��������� ������������������, ���� �� ��� ��������������� ����� <= evDATE
  -- 2 - ������ ��������� ������������������, ���� �� ��� ��������������� ����� <= evDATE � ��� �������� = 1 (��� � ��� evDate)
  Function has_register_history(pvcObjectType IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                               , pnObjID IN NUMBER -- ������������� ������������������� �������
                               , TypeRep IN CDCH.ccdchtype%TYPE -- ��� ������ 
                               , piruleMode In Integer  -- ��� ������� ��������� 0,1,2 (����������� ����, ��� ������ ��� ���������������)
                               , evDATE In DATE DEFAULT CD.get_lsdate) Return Pls_integer;
----------------------------------------------------------------------
-- Vct z.182475 
-- ��� ����������� � �������������� ����� ������� ��������
-- ���� ������ ��������� ������ - ������� ������� �������� �� ��������
-- � ���� ��������
Procedure ClearRegisteredHistory_svk(pn_agrId in Number);
----------------------------------------------------------------------
-- Vct 24.01.2019 z.190592 
-- �������� ������� ��������� � ������� ����������������� �������� � ���
-- ���������� 0, ���� �� ��������� � �������, 1 � �������� ������.
-- ����� cd_cdch.Document_Has_BKI_History(pn_docid in Number /* ������������� ��������� */ )
Function Document_Has_BKI_History(pn_docid in Number -- ������������� ���������
                                 ) return Number;
-----------------------------------------------------------------------
-- ������ ��������� ��������� �������� ��. ���������� � �������� �������� ��� �� ���
-- ��� ��������� ������� ������� ������� ����������� �������� ����������� ���������
Procedure Clear_Total_Doc_BKI_Hist(
                                  pn_docid_in in Number -- ������������� ���������
                                  );
-----------------------------------------------------------------------
-- Vct 25.01.2019 ��� ������ � ����� cdhistdc.fmb
-- ������ ��������� ��������� �������� ��. ���������� � �������� �������� ��� �� ���
-- � ����� ������ ������������ �������������� ����������� ��������� ��� ����������� �������� � ���
-- ���� � ����� �������� ������� ������������ ����������,
-- �������� ����� �� ��������� ������������ ���� �������
-- �����: cd_cdch.mk_Document_BKI_Ready_cf(pn_docid_in in Number, i_result_Out OUT Number, pc_error_message_out Out Varchar2);
Procedure mk_Document_BKI_Ready_cf(pn_docid_in in Number -- ������������� ���������
                                 , i_result_Out OUT Number -- ������
                                 , pc_error_message_out Out Varchar2
                                 );
-----------------------------------------------------------------------
-- ��������� ��������� ����� ������ ������ � ������� �����������
-- 0 - ����� ����, 1 - ����������  
Procedure SetReplaceData_Flag(pn_Flag In Integer);
-----------------------------------------------------------------------
-- ������� ��������� ����� ������ ������ � ������� �����������
-- ��� ������������� � �������� ����. ������
-- 0 - ����� ����, 1 - ����������  
-- ���������� ������������� �������� ����� � ���� ������ �����
-- cd_cdch.bf_setReplaceData_Flag(pn_Flag In Integer)
Function bf_setReplaceData_Flag(pn_Flag In Integer) Return Integer;
-----------------------------------------------------------------------
-- Vct 18.12.2020 - ������������ � ����������
-- Vct 28.06.2018 z.182475 �������� ������� ����������� ���� ��������� ���������� �������  ����� ��������� ����
Procedure DelAllRegisteredHistory(pn_agrID in Number -- ������������� ��������
                              , pd_dateStartFrom in Date
                              , pc_rpttype in Varchar2 -- ��� ������ TUTDF - 
                              );
----------------------------------------------------------------------
-- Vct 18.12.2020 - ������������ � ����������
-- Vct 28.06.2018 z.182475 �������� ������� ����������� ���� ��������� ���������� �������  ����� ��������� ����
-- � ��������� ����������
Procedure DelAllRegisteredHistory_AT(pn_agrID in Number -- ������������� ��������
                                   , pd_dateStartFrom in Date
                                   , pc_rpttype in Varchar2 -- ��� ������ TUTDF - 
                                    );                              
-------------------------------------------------------------------------
-- z.211947
-- ������ �������� ������� ���� ��������, ���������� �� ���������� ����
Procedure Clear_RegHistory_For_Day(pd_onDate in date , pc_reptype In varchar2);
-------------------------------------------------------------------------
-- ������ �������� ������� ���� ��������, ���������� �� ���������� ����
-- � ���������� ����������
Procedure Clear_RegHistory_For_Day_AT(pd_onDate in date , pc_reptype In varchar2, pn_result Out Number);                                    
-------------------------------------------------------------------------
end cd_cdch;
/
create or replace package body cd_cdch is

  ci_Zero constant pls_integer := 0;
  ci_False constant pls_integer := 0;
  --
  ci_SUCCESS constant Pls_integer :=0; -- �������� ����������
  ci_OTHER_ERROR constant Pls_integer := 8192;  -- ������ ������ (����������� �������)
-------------------------------------------------------------------
-- 31.12.2019 Vct
-- ���� ������ ������ - ��� ������� 
  bReplaceData_flag Boolean := False;
  

-- ��������� ��������� ����� ������ ������ � ������� �����������
-- 0 - ����� ����, 1 - ����������  
Procedure SetReplaceData_Flag(pn_Flag In Integer)
Is
Begin
  bReplaceData_flag := Not (coalesce(pn_Flag, ci_Zero) = ci_Zero);
End;  
-------------------------------------------------------------------
-- ������� ��������� ����� ������ ������ � ������� �����������
-- ��� ������������� � �������� ����. ������
-- 0 - ����� ����, 1 - ����������  
-- ���������� ������������� �������� ����� � ���� ������ �����
Function bf_setReplaceData_Flag(pn_Flag In Integer) Return Integer
Is
Begin
  SetReplaceData_Flag(pn_Flag => pn_Flag);
  Return Case True
         When bReplaceData_flag Then 1
         Else 0  
         End;
End;    
-------------------------------------------------------------------  
-- ��������� ������� "���������" ������ ����������� �������� ��� ���
Procedure ClearCDCH_TMPTBL(pvc_SEssionID In Varchar2)
is
Begin  
  DELETE FROM CDCH_TMP
  WHERE CCDCHSESSIONID = pvc_SEssionID; -- USERENV('SESSIONID');
  DELETE FROM CDCH_RQ_TMP
  WHERE CCDCHSESSIONID = pvc_SEssionID; -- USERENV('SESSIONID');
  DELETE FROM CDCH_D_TMP
  WHERE CCDCHSESSIONID = pvc_SEssionID; -- USERENV('SESSIONID');  
  DELETE FROM cdch_czo_tmp 
  WHERE CCDCHSESSIONID = pvc_SEssionID; -- USERENV('SESSIONID');  
  -- Vct 02.10.2019
  Delete From CDCH_CLIPAY_TMP;  -- ��������� �������
  -- Vct 08.11.2019
  Delete  cdch_clipayovr_tmp;
  Delete CDCH_CLIPAY_SQLOG_TMP;

  bReplaceData_flag := False; -- Vct 31.12.2019
  
End;  

-- ��������� ������� "���������" ������ ����������� �������� ��� ��� � ��������
Procedure ClearwCommitCDCH_TMPTBL(pvc_SEssionID In Varchar2)
is
Begin
  
  ClearCDCH_TMPTBL(pvc_SEssionID);
  COMMIT WORK WRITE BATCH NOWAIT;    
End;  
-- ��������� ������� "���������" ������ ����������� �������� ��� ��� � ���������� ����������
Procedure ClearAutoTranCDCH_TMPTBL(pvc_SEssionID In Varchar2)
is
  PRAGMA AUTONOMOUS_TRANSACTION;
Begin  
  ClearCDCH_TMPTBL(pvc_SEssionID);
--  COMMIT;    
  COMMIT WORK WRITE BATCH NOWAIT;  
End;  
--------------------------------------------------------------------
-- z.211947
-- ������ �������� ������� ���� ��������, ���������� �� ���������� ����
Procedure Clear_RegHistory_For_Day(pd_onDate in date , pc_reptype In varchar2)
Is
  vc_rpttype cdch.ccdchtype%Type;
Begin
  vc_rpttype := Upper(pc_reptype);
  -- ��������� �������� - cdch
  Delete /*+ cluster_by_rowid(t) */ From cdch t
  Where 1 = 1 
  And t.dcdchdate = pd_onDate --Cast( pd_onDate as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- �������� ������� ������������������ �������� (�������� PA).
  Delete /*+ cluster_by_rowid(p) */ From cdch_clipay p Where 1 = 1 
  And p.dcdch_registration = pd_onDate --sysdate --:d_reg
  And p.ccdchtype = vc_rpttype --'TUTDF'
  And p.icdchpa_issccode = 27 -- �������
  ;
  -- �������� ��. ����������
  Delete /*+ cluster_by_rowid(t) */  from cdch_d t  Where 1 = 1 
  And t.dcdchdate = pd_onDate -- :p1
  And t.ccdchtype = vc_rpttype --'TUTDF'
  ;
  ---  �������������� (����������� � ������) cdch_czo, v_czo_kb_1
  Delete /*+ cluster_by_rowid(t) */  from cdch_czo  t
  Where 1 = 1   
  And t.ccdchtype = vc_rpttype
  And t.dcdchdate = pd_onDate
  ;
  -- ������ ��������    - cdch_rq, V_CDMO_Z_KB
  Delete /*+ cluster_by_rowid(t) */ From cdch_rq t
  Where 1 = 1 
  And t.dcdchdate = pd_onDate
  And t.ccdchtype = vc_rpttype
  ;  
  -----------------------------------------------------------
  -- �������� ������ �� ��������� ������ ��� �������� ������
  ------------------------------------------------------------
  ClearCDCH_TMPTBL(sys_context('USERENV', 'SESSIONID'));
  
  
End;   
-- ������ �������� ������� ���� ��������, ���������� �� ���������� ����
-- � ���������� ����������
Procedure Clear_RegHistory_For_Day_AT(pd_onDate in date , pc_reptype In varchar2, pn_result Out Number) 
is
Begin
  Clear_RegHistory_For_Day(pd_onDate => pd_onDate , pc_reptype => pc_reptype);
  Commit;
  pn_result := ci_SUCCESS;
  Exception
    When Others Then
      pn_result := sqlcode;      
      cd_utl2s.TxtOut('Clear_RegHistory_For_Day_AT.1128:ERROR:'||cd_errsupport.format_ora_errorstack(true));
      Rollback;
End;
  
--------------------------------------------------------------------
-- Vct 31.12.2019
-- ��������� ������� �������� ������������������ �������� ��� �������� ������ � ������� �������
Procedure DelHistory_for_Replace
Is
Begin
  -- ��������� �������� - cdch
  Delete From cdch t
  Where 1 = 1 
    And (t.ncdchagrid , t.ccdchtype) In (
          Select tmp.ncdchagrid, tmp.ccdchtype
          From cdch_tmp tmp -- v_cdch_tmp tmp 
          --Where 1 = 1 AND T.cvcdchobjtype  = 'CAGREEMENT'
       );
  -------------------------------
  -- ������ ��������    - cdch_rq, V_CDMO_Z_KB
  Delete From cdch_rq t
  Where  1 = 1 And 
  (t.ncdchzid, t.ccdchtype) In (
     Select tmp.ncdchzid, tmp.ccdchtype
     From cdch_rq_tmp tmp
  );
  --------------------
  -- ����������� ��������� ������� -- cdch_d, v_cdh_doc_j
  Delete from cdch_d t
  Where 1 = 1 And
    (t.ncdchdid, t.ccdchtype) In (
        Select tmp.ncdchdid, tmp.ccdchtype
        From cdch_d_tmp tmp
    );

  --  -  �������������� (�����������) cdch_czo, v_czo_kb_1
  Delete from cdch_czo  t
  Where 1 = 1 And
     (t.ncdchiczoid, t.ccdchtype ) in (
          Select tmp.ncdchiczoid, tmp.ccdchtype
          From cdch_czo_tmp tmp
     );
     
  --   -- Vct 07.11.2019 - ������� ����������� ��������� PA
  Delete From CDCH_CLIPAY t
  Where 1 = 1 And 
  (t.ncdchpa_objid, t.ccdchtype) In (
        Select t.ncdchpa_objid, t.ccdchtype
        From cdch_clipay_tmp tmp
  );
  
End;
--------------------------------------------------------------------

-- ��������� ����������� ������ 
--�������� ������ �� ��������� ������ � ��������������� ���������� 
Procedure Register_all_reports(pvc_sessionID In Varchar2)
is
Begin
    
  IF bReplaceData_flag
    THEN
    DelHistory_for_Replace();
    bReplaceData_flag := False;
  End If;      
  ------------------------------------------------------------------------------------------------
  -- !!NB - ��������� � ������� �� ��������� ������ ��������� � ������������ � ������������ ������
  ------------------------------------------------------------------------------------------------
  /*
   ���� ����� ������������ merge ������ ��� insert, ��������� update �����
  */
  MERGE INTO cdch USING (
                          Select 
                        T.nvcdchobj_id --, T.cvcdchobjtype 
                        ,T.cvcdchreptype
                        ,T.dvcdchrepdate, T.cvcdchrepuser, T.dvcdchreptime, T.ivcdchastatus
                        From v_cdch_tmp T
                        WHERE 
                          T.cvcdchsessionid = pvc_sessionID
                        AND T.cvcdchobjtype  = 'CAGREEMENT'
                        -- Vct 28.10.2019 z.196306
                        -- �� ������������ ��������, �� ������� ������������� ������������ �������� PA
                        -- ��������������, ��� ����� ����������� �� ��� ����� ����������� ������ ������
                        -- cdch_clipayovr_tmp(ccdchtype,icdchpa_issccode,ncdchpa_objid) ...
                        And Not Exists(
                              Select Null
                              From cdch_clipayovr_tmp t_ovr
                              Where 
                                t_ovr.icdchpa_issccode = 27
                              And t_ovr.ccdchtype = t.cvcdchreptype
                              And t_ovr.ncdchpa_objid = T.nvcdchobj_id
                                )
   ) T 
   ON  (
       cdch.ncdchagrid = T.nvcdchobj_id 
   And cdch.ccdchtype = T.cvcdchreptype
   And cdch.dcdchdate = T.dvcdchrepdate
       )
  -- Vct 23.06.2020 z.204079 - ��������� ��������� ����������� ��� �������� ���������.     
  WHEN MATCHED THEN 
    UPDATE SET ccdchuser = cvcdchrepuser, dcdchsysdate=dvcdchreptime, icdchastatus = ivcdchastatus    
  WHEN NOT MATCHED THEN
    INSERT (ncdchagrid,  ccdchtype,    dcdchdate,    ccdchuser,    dcdchsysdate, icdchastatus)
    VALUES (nvcdchobj_id,cvcdchreptype, dvcdchrepdate,cvcdchrepuser,dvcdchreptime,ivcdchastatus )  
  ;
  ------------------
  MERGE INTO cdch_rq USING (
                          Select 
                        T.nvcdchobj_id --, T.cvcdchobjtype 
                        ,T.cvcdchreptype
                        ,T.dvcdchrepdate, T.cvcdchrepuser, T.dvcdchreptime, T.ivcdchastatus
                        From v_cdch_tmp T
                        WHERE 
                          T.cvcdchsessionid = pvc_sessionID
                        AND T.cvcdchobjtype  = 'CREQUEST'
   ) T 
   ON  (
       cdch_rq.ncdchzid = T.nvcdchobj_id 
   And cdch_rq.ccdchtype = T.cvcdchreptype
   And cdch_rq.dcdchdate = T.dvcdchrepdate
       )  
  -- Vct 23.06.2020 z.204079 - ��������� ��������� ����������� ��� �������� ���������.
  WHEN MATCHED THEN 
    UPDATE SET ccdchuser = cvcdchrepuser, dcdchsysdate=dvcdchreptime, icdchastatus = ivcdchastatus    
  WHEN NOT MATCHED THEN
    INSERT (ncdchzid,  ccdchtype,    dcdchdate,    ccdchuser,    dcdchsysdate, icdchastatus)
    VALUES (nvcdchobj_id,cvcdchreptype, dvcdchrepdate,cvcdchrepuser,dvcdchreptime,ivcdchastatus )  
  ;  
  -------------------
  MERGE INTO cdch_d USING (
                        Select 
                        T.nvcdchobj_id --, T.cvcdchobjtype 
                        ,T.cvcdchreptype
                        ,T.dvcdchrepdate, T.cvcdchrepuser, T.dvcdchreptime, T.ivcdchastatus
                        From v_cdch_tmp T
                        WHERE 
                          T.cvcdchsessionid = pvc_sessionID
                        AND T.cvcdchobjtype  = 'CJDOCUMENT'
   ) T 
   ON  (
       cdch_d.ncdchdid = T.nvcdchobj_id 
   And cdch_d.ccdchtype = T.cvcdchreptype
   And cdch_d.dcdchdate = T.dvcdchrepdate
       )  
  -- WHEN MATCHED THEN 
  --  UPDATE SET ccdchuser = cvcdchrepuser, dcdchsysdate=dvcdchreptime, icdchastatus = ivcdchastatus    
  WHEN NOT MATCHED THEN
    INSERT (ncdchdid,  ccdchtype,    dcdchdate,    ccdchuser,    dcdchsysdate, icdchastatus)
    VALUES (nvcdchobj_id,cvcdchreptype, dvcdchrepdate,cvcdchrepuser,dvcdchreptime,ivcdchastatus )  
  ;  
  -------------------
  -- TODO Vct 31.12.2019 - ��� ����������� �������������, � ��� �� �����, ������...
  -- �.�. ��� ������ � ����...
  -------------------
  -- Vct 02.10.2019 - ����������� �������� AP
  MERGE INTO CDCH_CLIPAY USING (
    Select 
      t.icdchpa_issccode  -- ��� ���������� (�� ��������� - 27 - ��� ��������� ���������)
    , t.ncdchpa_objid  -- ��� ������� (������������� ���������� �������� ��� 27 ����������
    , t.ccdchtype      -- ��� ������ - TUTDF/CAIS/NCB-CHDTF/XML1...
    , t.icdchpa_paynum99  -- ����� ������� � ��������� 1 - 99 (���� �������� PA), ��� ���� 1 �������� PA
    , t.dcdch_paydate  -- ���� �������, - ���� 5 �������� PA,
    , t.mcdch_fulldaypay  -- ����� �������������� �������, ������������, ����� 10, ������������� ����� �����, ���� ��������� 9,999,999,999 �� ������������ ����� ���������, ���� 2 �������� PA 
    , t.mcdch_daypayact  -- ����� �������������� �������, �� ����������� ������������ �������� ������ ����� 30 ����, ������������� ���������� ���� 2), ���� 3 �������� PA --
    , t.ccdch_curr  -- ������ ������� ������������, ����� 3 (RUB �� ���������), ���� 4 �������� PA
  -- ����� ����� ��������� ���� ��� ��������� ���� 6 �������� PA, ���� ������, ��� ����� ����� ���������...
  -- 6 - ����� ������� F/P F - ���������, P - �� ���������
  --
    , t.mcdch_totalact12   -- ��������� ������ ����������� �������� �� 12 ������� �� ����������� ������������ �������� ������ ����� 30 ����, ����� 10, ���� 7 �������� PA     
    , Coalesce(t.ccdch_payvol,'F') as ccdch_payvol
    From CDCH_CLIPAY_TMP t
  ) T
  ON (
    CDCH_CLIPAY.icdchpa_issccode = T.icdchpa_issccode
    And CDCH_CLIPAY.ncdchpa_objid = T.ncdchpa_objid
    And CDCH_CLIPAY.ccdchtype = T.ccdchtype
    And CDCH_CLIPAY.dcdch_paydate = T.dcdch_paydate  
  )
  WHEN NOT MATCHED THEN 
    INSERT (
     icdchpa_issccode, ncdchpa_objid, ccdchtype, icdchpa_paynum99, dcdch_paydate, mcdch_fulldaypay, mcdch_daypayact, ccdch_curr, mcdch_totalact12, ccdch_payvol 
     )
    VALUES(t.icdchpa_issccode, t.ncdchpa_objid, t.ccdchtype, t.icdchpa_paynum99, t.dcdch_paydate, t.mcdch_fulldaypay
          , t.mcdch_daypayact, t.ccdch_curr, t.mcdch_totalact12, t.ccdch_payvol
          );
  ---- 
       
end;
-----------------------------------
-- ������� ���������� 1, ���� ���������� ������� ������������������ � ����� ������ �� 
-- �������� ��� ������� ���, 0 � ��������� ������
Function session_data_exists(pvc_sessionID in Varchar2) Return Integer
is
  iretval Integer := 0;
Begin
  Select Count(*) Into iretval
  From v_cdch_tmp T  
  WHERE 
    T.cvcdchsessionid = pvc_sessionID 
  And rownum = 1;
  Return iretval;   
End;    
----------------------------------------------------------------------
-- ������� ���������� �������������� � ������ ��� ��� ������ �� "���������" �������, 
----------------------------------------------------------------------
-- �-��� ���������� ������, ���� ��� ������ �� �������� ��� ����������� ��� ������������� ��������������� ������ �� �����
function dont_registerobj(AgrID in NUMBER, -- ������������� ��������������� �������
                          TypeRep In Varchar2 -- CDCH_TMP.ccdchtype%TYPE, -- ��� ������ 
                         -- ,pvc_caller in Varchar2  -- ���������� (���� �� ������������) ��� ������������� ������� ������� ����������� � ��������� ��������� ��������� � ����� ������
                         ) Return boolean
Is
Begin
  Return ((AgrID IS NULL) OR NOT (TypeRep IN ('TUTDF','CAIS'
                                             , 'CR_XML' -- Vct 29.04.2015 (z.152775) - ��������� ��� ������� Credit_Registry (�� ����, ����� ��������� ��������������� ��� ����� ������� �����)
                                             ,'NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1'
                                               
                                            )
                                  ) 
         );
End;                           
                                                    
-- ������ �������� �� ��������� ������� CDCH_TMP - ��������������� ��� ��������� ���������
FUNCTION Insert_CDCH_TMP(AgrID NUMBER,
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- ���������� ���������� ��� ������������� � �������������� ������:
--  PRAGMA AUTONOMOUS_TRANSACTION; 
BEGIN
  --IF (AgrID IS NULL) OR NOT(TypeRep IN ('TUTDF','CAIS','NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1')) THEN
  PRAGMA INLINE(dont_registerobj,'YES');
  IF dont_registerobj(AgrID, TypeRep) THEN
    RETURN 0;
  END IF;
  INSERT INTO CDCH_TMP(ncdchagrid,ccdchtype,dcdchdate,ccdchsessionid,ICDCHASTATUS)
  VALUES(AgrID,TypeRep,evDATE,COALESCE(SID,cd_utl2s.Get_SessionID_char),pnAStatus);

  RETURN 1;
EXCEPTION WHEN OTHERS THEN
  RETURN 0;
END;
----------------------------------------------------------------------
-- ������ ������ �� ��������� ������� CDCH_RQ_TMP - ��������������� ����������� ��� ������
FUNCTION Insert_CDCH_RQ_TMP(pZID NUMBER, -- ���������� ����� ������
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- ���������� ���������� ��� ������������� � �������������� ������:
--  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- IF (pZID IS NULL) OR NOT(TypeRep IN ('TUTDF','CAIS','NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1')) THEN
  PRAGMA INLINE(dont_registerobj,'YES');
  IF dont_registerobj(pZID, TypeRep) THEN    
    RETURN 0;
  END IF;
  
  INSERT INTO CDCH_RQ_TMP(ncdchZID,ccdchtype,dcdchdate,ccdchsessionid,ICDCHASTATUS)
  VALUES(pZID,TypeRep,evDATE,COALESCE(SID,cd_utl2s.Get_SessionID_char),pnAStatus);

  RETURN 1;
EXCEPTION WHEN OTHERS THEN
  RETURN 0;
END;
----------------------------------------------------------------------
-- ������ ��������� �� ��������� ������� CDCH_D_TMP -- ��������������� ����������� ����������� ����������
FUNCTION Insert_CDCH_D_TMP(pDID NUMBER, -- ���������� ����� ���������
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- ���������� ���������� ��� ������������� � �������������� ������:
--  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- IF (pDID IS NULL) OR NOT(TypeRep IN ('TUTDF','CAIS','NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1')) THEN
  PRAGMA INLINE(dont_registerobj,'YES');
  IF dont_registerobj(pDID, TypeRep) THEN
    RETURN 0;
  END IF;
  INSERT INTO CDCH_D_TMP(ncdchDID,ccdchtype,dcdchdate,ccdchsessionid,ICDCHASTATUS)
  VALUES(pDID,TypeRep,evDATE,COALESCE(SID,cd_utl2s.Get_SessionID_char), pnAStatus);

  RETURN 1;
EXCEPTION WHEN OTHERS THEN
  RETURN 0;
END;
----------------------------------------------------------------------
-- ������ ����������� �� ��������� ������� CDCH_CZO_TMP
FUNCTION Insert_CDCH_CZO_TMP(pDID in NUMBER, -- ������������� ������� (�����������)
                            SID in Varchar2, --CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep in Varchar2, -- CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- ���������� ���������� ��� ������������� � �������������� ������:
--  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  -- IF (pDID IS NULL) OR NOT(TypeRep IN ('TUTDF','CAIS','NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1')) THEN
  PRAGMA INLINE(dont_registerobj,'YES');
  IF dont_registerobj(pDID, TypeRep) THEN
    RETURN 0;
  END IF;
  INSERT INTO CDCH_CZO_TMP(NCDCHICZOID,ccdchtype,dcdchdate,ccdchsessionid,ICDCHASTATUS)
  VALUES(pDID,TypeRep,evDATE,COALESCE(SID,cd_utl2s.Get_SessionID_char), pnAStatus);

  RETURN 1;
EXCEPTION WHEN OTHERS THEN
  RETURN 0;
END;

----------------------------------------------------------------------
/*
  ----
  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� �������
  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
  ----
*/
-- ����� �-��� ��������������� ����������� ������ �� ��������, �������������� � ���
-- ���������� ������� ��� pvcObjectType:
--  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
--  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� �������
--  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
Function Register_cdch_tempobj(pvcObjectType IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                              , nObjID In Number -- �������� ������������� ��������������� �������
                              , pvcSessionID in Varchar2 -- ������������� ������ ������������
                              , pvcReportType in Varchar2 -- ������� ��� ���� ������
                              , pnAStatus in Integer Default 0 -- ���. ������, (1 - ��������� �� ����������� �������)
                              , pdEventDate in Date Default CD.Get_LSDATE -- ����, ������� ������������ �����
                              ) Return Pls_integer 
IS
-- ���������� ���������� ��� ������������� � �������������� ������:
  PRAGMA AUTONOMOUS_TRANSACTION;
  vcLocalPObjType T_CDCH_CODENAME;
  iretval pls_integer := 0;
BEGIN
  vcLocalPObjType := Upper(pvcObjectType);
  CASE vcLocalPObjType
    WHEN cc_CDAGR_Alias THEN -- 'CAGREEMENT'; -- ��� ����������� ��������� ���������      
     iretval := Insert_CDCH_TMP(AgrID => nObjID,
                                SID => pvcSessionID,
                                TypeRep => pvcReportType,
                                pnAstatus => COALESCE(pnAStatus,0),
                                evDATE => pdEventDate
                                );
                                
    WHEN cc_CDREQUEST_Alias THEN -- 'CREQUEST'; -- ��� ����������� ������ �� �������
      iretval := Insert_CDCH_RQ_TMP(pZID => nObjID, -- ���������� ����� ������
                                    SID => pvcSessionID,
                                    TypeRep => pvcReportType,
                                    pnAstatus => COALESCE(pnAStatus,0),
                                    evDATE => pdEventDate);
                                    
    WHEN cc_CDJDOC_Alias THEN -- 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
      iretval := Insert_CDCH_D_TMP(pDID => nObjID, -- ���������� ����� ���������
                                   SID => pvcSessionID,
                                   TypeRep =>  pvcReportType,
                                   pnAstatus => COALESCE(pnAStatus,0),
                                   evDATE => pdEventDate);
    -- ����������� ����� �� ������������� ��� cc_CDJDOC_Alias, �� ������������ ����������� �����
    WHEN cc_PROVISION_Alias THEN
      iretval := Insert_CDCH_CZO_TMP(pDID => nObjId, SID => pvcSessionID,
                                    TypeRep => pvcReportType, pnAStatus => COALESCE(pnAStatus,0),
                                     evDATE => pdEventDate);
                                         
  ELSE 
    cd_utl2s.TxtOut('cd_cdch.rct.1: �� �������� ��� ���� ������������ ������� pvcObjectType='||pvcObjectType);    
  END CASE;   

  COMMIT WORK WRITE BATCH NOWAIT;   

  Return iretval;
EXCEPTION WHEN OTHERS THEN
  Rollback;
  RETURN 0;  
End;                                        
----------------------------------------------------------------------
-- �-��� ���������� ��������� ���� ����������� ������� � ������� ���
-- ���������� �������� ��� pvcObjectType:
--  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- ��� ����������� ��������� ���������
--  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- ��� ����������� ������ �� ��������
--  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- ��� ����������� ����������� ���������� (����������� � �.�.)
--  cc_PROVISION_Alias T_CDCH_CODENAME := 'CPROVISION'; -- ��� ����������� �������������
Function Get_LastRegisterDate(pvcObjectType in Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                              , pnObjID in NUMBER -- ������������� ������������������� �������
                              , TypeRep in CDCH.ccdchtype%TYPE -- ��� ������ 
                              , evDATE in DATE DEFAULT CD.get_lsdate) RETURN DATE 
IS
  --dRetval DATE;
  -- ��� ������������������ ��������� ���������
  Function get_cdagr_lastregdate(
                                 pnObjID2 IN NUMBER -- ������������� ������������������� �������
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                               , evDATE2 DATE DEFAULT CD.get_lsdate
                                ) Return Date
  -------------------------------                                  
  is 
    dret date;
  Begin      
    IF pnObjID2 IS NULL THEN
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CAGREEMENT' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        -- AND T.nvcdchobj_id = pnObjID 
        AND T.dvcdchrepdate <= evDATE2;
    ELSE
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CAGREEMENT' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        AND T.nvcdchobj_id = pnObjID2
        AND T.dvcdchrepdate <= evDATE2;
    END IF;    
    Return dret;
  end;                            
---------  
  -- ��� ������������������ ������ �� ��������
  Function get_cdreq_lastregdate(
                                 pnObjID2 IN NUMBER -- ������������� ������������������� �������
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                               , evDATE2 DATE DEFAULT CD.get_lsdate
                                ) Return Date
  is 
    dret date;
  Begin      
    IF pnObjID2 IS NULL THEN
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CREQUEST' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        -- AND T.nvcdchobj_id = pnObjID 
        AND T.dvcdchrepdate <= evDATE2;
    ELSE
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CREQUEST' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        AND T.nvcdchobj_id = pnObjID2
        AND T.dvcdchrepdate <= evDATE2;
    END IF;    
    Return dret;
  end;                            
---------  
  -- ��� ������������������ ��. ����������
  Function get_jdoc_lastregdate(
                                 pnObjID2 IN NUMBER -- ������������� ������������������� �������
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                               , evDATE2 DATE DEFAULT CD.get_lsdate
                                ) Return Date
  is 
    dret date;
  Begin      
    IF pnObjID2 IS NULL THEN
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CJDOCUMENT' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        -- AND T.nvcdchobj_id = pnObjID 
        AND T.dvcdchrepdate <= evDATE2;
    ELSE
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CJDOCUMENT' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        AND T.nvcdchobj_id = pnObjID2
        AND T.dvcdchrepdate <= evDATE2;
    END IF;    
    Return dret;
  end;   
---------
 -- ����������� ����� �� ������������� ��� ��. ���������, �� ������������ ����������� �����
  -- ��� ������������������ �����������                           
  Function get_czo_lastregdate(
                                 pnObjID2 IN NUMBER -- ������������� ������������������� �������
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                               , evDATE2 DATE DEFAULT CD.get_lsdate
                                ) Return Date
  is 
    dret date;
  Begin      
    IF pnObjID2 IS NULL THEN
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CPROVISION' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        -- AND T.nvcdchobj_id = pnObjID 
        AND T.dvcdchrepdate <= evDATE2;
    ELSE
      SELECT MAX(T.dvcdchrepdate ) INTO dRET
      FROM v_cdch T
      WHERE 1 = 1
        AND T.cvcdchobjtype = 'CPROVISION' --upper(pvcObjectType)
        AND T.cvcdchreptype = upper(TypeRep2)
        AND T.nvcdchobj_id = pnObjID2
        AND T.dvcdchrepdate <= evDATE2;
    END IF;    
    Return dret;
  end;     
  
---------  
BEGIN
   
  RETURN CASE Upper(pvcObjectType)
           WHEN cc_CDAGR_Alias THEN get_cdagr_lastregdate(pnObjID, TypeRep, evDATE)
           WHEN cc_CDREQUEST_Alias THEN get_cdreq_lastregdate(pnObjID, TypeRep, evDATE)
           WHEN cc_CDJDOC_Alias THEN   get_jdoc_lastregdate(pnObjID, TypeRep, evDATE)
           WHEN cc_PROVISION_Alias THEN get_czo_lastregdate(pnObjID, TypeRep,evDATE)  
         END;
END;                                  
----------------------------------------------------------------------
-- �-��� ��������� 1, ���� ������ ��� ��� ��� ��������������� c ����� ������ ������� ���������,
-- ������������� ���������� piruleMode
-- �������� piruleMode: 
-- 0 - ������ ��������� ������������������, ���� �� ��������������� � ��� = evDATE
-- 1 - ������ ��������� ������������������, ���� �� ��� ��������������� ����� <= evDATE
-- 2 - ������ ��������� ������������������, ���� �� ��� ��������������� ����� <= evDATE � ��� �������� = 1 (��� � ��� evDate)
Function has_register_history(pvcObjectType IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                             , pnObjID IN NUMBER -- ������������� ������������������� �������
                             , TypeRep IN CDCH.ccdchtype%TYPE -- ��� ������ 
                             , piruleMode In Integer  -- ��� ������� ��������� 0,1,2 (����������� ����, ��� ������ ��� ���������������)
                             , evDATE In DATE DEFAULT CD.get_lsdate) Return Pls_integer
is

  ci_Zero constant Pls_Integer := 0;
  ci_ONE constant Pls_integer := 1;
  ci_Two constant Pls_Integer := 2;
  ci_RULE_REQUEST constant Pls_Integer := 4; -- ��� ���� �������� ����� ������� ��������� ����� ������� �� �������� 
                                    -- ���������� � ���������� ��������� �� �������� (� ������������ �� ��������� �������) 
  ------------------------------------------------------------------------------------------------------------------------
  ci_RULE_REQUEST2 constant Pls_Integer := 8; -- ��� ���� �������� ����� ������� ��������� ����� ������� �� �������� 
                                     -- ���������� � ���������� ��������� �� �������� (� ������������ �� ��������� �������) 
  -------------------------------------------------------------------------------------------------------------------------
  ci_noRecalcState constant Pls_integer := 0;
  ci_RecalcState constant Pls_integer := 1;
  --------------------------------------                                    
  --
  ilocalAttr Pls_integer;

  -- ������� 0 - ���� ����������� ��������� � �������� �� ����.
  Function rule_zero(pvcObjectType2 IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                    , pnObjID2 IN NUMBER -- ������������� ������������������� �������
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                    , evDATE2 In DATE) Return Pls_integer
  is
   dateRg date;                                 
  Begin
     dateRg := Get_LastRegisterDate(pvcObjectType2, pnObjID2, TypeRep2, evDATE2);                              
     RETURN CASE cd_utl2s.is_equal(dateRg,evDate2) WHEN True Then ci_One Else ci_Zero End;
  End;                    
  -- ������� 1 - ���� ����������� <= evDate
  Function rule_one(pvcObjectType2 IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                    , pnObjID2 IN NUMBER -- ������������� ������������������� �������
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                    , evDATE2 In DATE) Return Pls_integer
  is
   dateRg date;                                 
  Begin
     dateRg := Get_LastRegisterDate(pvcObjectType2, pnObjID2, TypeRep2, evDATE2);                              
     RETURN CASE dateRg < evDate2 OR cd_utl2s.is_equal(dateRg,evDate2) WHEN True Then ci_One Else ci_Zero End;
  End;      
  -- ������� 2 - ���� ������ � ���. �������� 1 � �������������� ���
  Function rule_two(pvcObjectType2 IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                    , pnObjID2 IN NUMBER -- ������������� ������������������� �������
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                    , evDATE2 In DATE) Return Pls_integer
  is
    nretval Pls_integer := ci_Zero;
    vcObjectType_loc T_CDCH_CODENAME;
  begin                    
    vcObjectType_loc := upper(pvcObjectType2);
    CASE vcObjectType_loc
      WHEN cc_CDAGR_Alias THEN
        Select Count(*) Into nretval From dual 
        Where 
          Exists(
                 Select  * From  v_cdch T
                 WHERE 1 = 1
                 AND T.cvcdchobjtype = 'CAGREEMENT' 
                 AND T.cvcdchreptype = upper(TypeRep2)
                 AND T.nvcdchobj_id = pnObjID2
                 AND (T.dvcdchrepdate <= evDATE2 
                       AND case when T.dvcdchrepdate < evDATE2 and T.ivcdchastatus = 1 then 1
                                when  T.dvcdchrepdate = evDATE2 then 1
                           end = 1
                     ) 
                );
    ----
      WHEN cc_CDREQUEST_Alias THEN
        Select Count(*) Into nretval From dual 
        Where 
          Exists(
                 Select  * From  v_cdch T
                 WHERE 1 = 1
                 AND T.cvcdchobjtype = 'CREQUEST'
                 AND T.cvcdchreptype = upper(TypeRep2)
                 AND T.nvcdchobj_id = pnObjID2
                 AND (T.dvcdchrepdate <= evDATE2 
                       AND case when T.dvcdchrepdate < evDATE2 and T.ivcdchastatus = 1 then 1
                                when  T.dvcdchrepdate = evDATE2 then 1
                           end = 1
                     ) 
                );                  
    ----
      WHEN cc_CDJDOC_Alias THEN
        Select Count(*) Into nretval From dual 
        Where 
          Exists(
                 Select  * From  v_cdch T
                 WHERE 1 = 1
                 AND T.cvcdchobjtype = 'CJDOCUMENT'
                 AND T.cvcdchreptype = upper(TypeRep2)
                 AND T.nvcdchobj_id = pnObjID2
                 AND (T.dvcdchrepdate <= evDATE2 
                       AND case when T.dvcdchrepdate < evDATE2 and T.ivcdchastatus = 1 then 1
                                when  T.dvcdchrepdate = evDATE2 then 1
                           end = 1
                     ) 
                );   
      WHEN cc_PROVISION_Alias THEN
        Select Count(*) Into nretval From dual 
        Where 
          Exists(
                 Select  * From  v_cdch T
                 WHERE 1 = 1
                 AND T.cvcdchobjtype = 'CPROVISION'
                 AND T.cvcdchreptype = upper(TypeRep2)
                 AND T.nvcdchobj_id = pnObjID2
                 AND (T.dvcdchrepdate <= evDATE2 
                       AND case when T.dvcdchrepdate < evDATE2 and T.ivcdchastatus = 1 then 1
                                when  T.dvcdchrepdate = evDATE2 then 1
                           end = 1
                     ) 
                );                                      
    ELSE 
      Null;  -- ������� �� �������, �� ������ ��������
    END CASE;  
    return nretval;
  end;  

  -- Vct 09.02.2016 -- ���� ������ ��� ������ �� ������� (������� IP � TUTDF)
  -- z.160830
  Function rule_REQUEST16(
                          pvcObjectType2 IN Varchar2 -- ��� ��������������� �������, �� ������ �������� ..._Alias
                        , pnObjID2 IN NUMBER -- ������������� ������������������� �������
                        , TypeRep2 IN CDCH.ccdchtype%TYPE -- ��� ������ 
                        , evDATE2 In DATE 
                        , pi_recalc_state in Pls_integer    
                         ) Return Pls_integer 
  Is
    SUBTYPE T_AGR_DEFAULTFLAG is Varchar2(2); -- ��� ��� ����� "�������" �� ��������

    SUBTYPE T_AGR_STATUS is "cda".ICDASTATUS%Type; -- ������ ��������
    SUBTYPE T_AGR_NUMBER is  "cda".NCDAAGRID%Type; -- ����� ��������

    ci_AGRCLOSED_STATUS constant T_AGR_STATUS := 3; -- ��������� ������� ������
    ci_AGRREFUSED_STATUS constant T_AGR_STATUS := 4; -- �������� -!!! ��� ������!!, 4 - ��� ������� � ������� ������! 
    ci_AGRDRAFT_STATUS constant T_AGR_STATUS := 0; -- ��������
    ci_AGRPRELIMINARY_STATUS constant T_AGR_STATUS := 1; -- ��������������� (��������)
    ci_AGRWORKING_STATUS constant T_AGR_STATUS := 2; -- ���������� �������
    
    nretval Pls_integer := ci_Zero;
    vcObjectType_loc T_CDCH_CODENAME;  
    iApproved_lc Integer; -- ���� ����������� ������� � ��������� ������ ��������
    iClosed_lc Integer; -- ���� ����������� ������� � ��������� �������� ������
    iToday_lc Integer;  -- ���� ����������� ������� �������
    -- Vct 11.03.2019 z.191849
    iRefused_lc Integer; -- ���� ����������� �������� ������
    
    -- Vct 05.12.2016 z.167590 - �������� ������ �� ��������� ��������
    iZeroStatus_lc Integer; -- ������ ����������� �� �������� 0
    dlast_registered Date;  -- ���� ��������� ����������� ������
    dRefuseDate_lc Date; -- ���� ������ �� ������
    ----------------
    iAgrStatus_lc T_AGR_STATUS; -- ������ �������� - (3 - ������)
    iAPPRVAL_lc Integer; -- ������ ��������� ������
    
    dcdaclosedDate_lc Date; -- ���� �������� ��������
    vnAgrID T_AGR_NUMBER; -- ����� ���������� ��������
    
    -- TODO !!!!- �������� � ������� ������� ���������!!!!!
    vc_default120 T_AGR_DEFAULTFLAG; --cd_types.TVCHAR2000; --T_AGR_DEFAULTFLAG;
    vc_temp_default_flag cd_types.TErrorString;
    
  Begin
    vcObjectType_loc := upper(pvcObjectType2);
    CASE vcObjectType_loc
      WHEN cc_CDREQUEST_Alias THEN
        -- �������� ���������� ����������� ������
        -----------------------------------------------------------------------------
        -- ������ �������� ���� ���, ���� ��� ��������,
        -- ���� ���, ����� ������� ������ � ������, ���� ������� � �������� �������.
        -- �� ������� ������ ���� ��������� ������� �� ����������� - ������ ������� � ������������ �������
        -----------------------------------------------------------------------------
      Select  /*+ cd_cdch.rule_REQUEST16.1 */
         --  COUNT(*) as ntotal, -- ����� ��������������� �������
          SUM(CASE WHEN T.ivcdchastatus = 1 then 1 END) as napproved -- ����������� ������� � ������� "������ ��������"
        , SUM(CASE WHEN T.ivcdchastatus = 3 then 1 END) as nClosed   -- ����������� ������� � ��������� ������� ������
        , SUM(CASE WHEN trunc(T.dvcdchrepdate) = evDATE2 THEN 1 END) ntoday -- ������ ����������� �������
        , SUM(CASE WHEN T.ivcdchastatus = 0 then 1 END) as nZeroStatus   -- ����������� �� �������� 0
        -- Vct 11.03.2019
        , SUM(CASE WHEN T.ivcdchastatus = 2 then 1 END) as nRefusedStatus   -- ����������� �� �������� 2 (��������)
        , MAX(dvcdchrepdate) dlastRegDate
      Into  iApproved_lc, iClosed_lc, iToday_lc,iZeroStatus_lc, iRefused_lc
          , dlast_registered
      From  v_cdch T
      Where 1 = 1
        AND T.cvcdchobjtype = 'CREQUEST'
        AND T.cvcdchreptype = upper(TypeRep2) --'TUTDF' 
        AND T.nvcdchobj_id = pnObjID2 --14339 --
        AND (T.dvcdchrepdate <= evDATE2 -- Date '2016-02-09' --
            );    
       ----------------------------------
       -- ������� ��������� �������� �� ������
      IF COALESCE(iToday_lc,0) = 0 
        THEN -- ������ ������� �� ���������� � �����, ������� �� ������
        Begin 
          Select /*+ cd_cdch.rule_REQUEST16.2 */
                 COALESCE(cda.ICDASTATUS,0), COALESCE(t_kb.APPRVAL,0)
          ,cda.NCDAAGRID, cda.DCDACLOSED -- Vct 02/08/2016  
          , Trunc(t_kb.DZDATERFSD) -- ���� ������ (����������� �� ��������, ������� trunc, Vct 11.03.2019)
          /*      
          -- Vct 17/02/2016 z.161129
          , CASE WHEN COALESCE(cda.ICDASTATUS,0) NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS)
                      AND COALESCE(t_kb.APPRVAL,0) = 1
                  THEN
                 COALESCE(cdrep_util3.get_agrdefault_flag(pn_agrid => t_kb.NZAGRID, pd_ondate => evDATE2),'Z')
                 ELSE 'Z'         
            END  */
          Into iAgrStatus_lc, iAPPRVAL_lc, vnAgrID, dcdaclosedDate_lc 
               , dRefuseDate_lc         
           --, vc_default120             
          From v_cdmo_z_kb t_kb, cda_mf cda
          Where 
                 t_kb.IZID = pnObjID2
            AND  t_kb.NZAGRID = cda.NCDAAGRID(+) -- (+) Vct 05.12.2016 z.167590
            ;
          IF vnAgrID IS NULL 
            THEN
            iAgrStatus_lc := 0;
            iAPPRVAL_lc := 0;              
          END IF;      
        Exception
          WHEN NO_DATA_FOUND THEN
            iAgrStatus_lc := 0;
            iAPPRVAL_lc := 0;
            -- vc_default120 := 'Z';
          WHEN OTHERS THEN 
              RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR,'cd_cdch.656:������ ������� � ������� �� ������-<'
                                    ||pnObjID2||'>:'||cd_errsupport.format_ora_errorstack(true));              
        End;
        vc_default120 := 'Z';
        -- Vct 17/02/2016 z.161129
        IF  iAPPRVAL_lc = 1 -- ������ ��������
            AND (iAgrStatus_lc = ci_AGRWORKING_STATUS
                 OR (iAgrStatus_lc = ci_AGRCLOSED_STATUS And dcdaclosedDate_lc > evDATE2)
                 ) -- Vct ��� ��������� �������� ����� ��������, ���� ������� ���� ������ ���� �������� ��������
                     --    NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- ������� �� ������
            AND iAPPRVAL_lc = 1 -- ������ ��������
          THEN -- ������� �� ������ - ������� ��������� �������    
          vc_temp_default_flag := COALESCE(cdrep_util3.get_agrdefault_flag(pn_agrid => vnAgrID, pd_ondate => evDATE2,pi_recalc_flag => pi_recalc_state),'Z');
          IF vc_temp_default_flag NOT IN ('Z','D')
            THEN
              cd_utl2s.TxtOut('cd_cdch.668:������ ��� ����������� ����� �������. ������-<'
                                     ||pnObjID2||'>, ������� - <'||vnAgrID
                                     -- Vct 16.08.2016 �������� ����� ������ �����
                                     ||'>: pd_date ='||to_char(evDATE2, 'DD.MM.YYYY')
                                     ||' pi_recalc_state='||pi_recalc_state -- Vct 15.08.2017
                                     ||' vc_temp_default_flag='||vc_temp_default_flag);
                                     
              RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR,'cd_cdch.668:������ ��� ����������� ����� �������. ������-<'
                                     ||pnObjID2||'>, ������� - <'||vnAgrID
                                     -- Vct 16.08.2016 �������� ����� ������ �����
                                     ||'>: pd_date ='||to_char(evDATE2, 'DD.MM.YYYY')
                                     ||' pi_recalc_state='||pi_recalc_state -- Vct 15.08.2017
                                     ||' vc_temp_default_flag='||vc_temp_default_flag
                                     ||':'
                                     ||cd_errsupport.format_ora_errorstack(true)
                                     );
          END IF;    
          vc_default120 := vc_temp_default_flag;
        END IF;    
      END IF;
       ----------------------------------   
       --   
       nretval := CASE
                    WHEN iToday_lc > 0 THEN 1 -- ������ ������� ��� ����������
                    WHEN iAgrStatus_lc IN ( ci_AGRCLOSED_STATUS --, ci_AGRREFUSED_STATUS -- rem Vct 11.03.2019
                                          )
                         AND ((iToday_lc > 0) OR (iClosed_lc > 0))
                        THEN 1 -- ����� ���������� ������ � ��������� ������� ������
                            
                    WHEN 1 = 1 -- And iAgrStatus_lc NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- rem Vct 02.08.2016
                        AND iAPPRVAL_lc = 1 
                        AND ((iToday_lc > 0) OR iApproved_lc > 0 )
                        AND vc_default120 = 'Z' -- ��� ��������� �������
                        THEN 1 -- ����� ���������� ������ � ��������� �������� 
                    WHEN vc_default120 = 'D' -- ���� ��������� �������
                         -- AND iAgrStatus_lc NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- rem Vct 02.08.2016
                         AND iToday_lc > 0 
                         THEN 1 -- ������� �� ������ � ��������� ������� � ������ ������� ��� ����������
                    -- Vct 05.12.2015 z.167590 - ����� ���������� �������� ������
                    --WHEN dRefuseDate_lc <= evDATE2 AND (iZeroStatus_lc > 0 OR iToday_lc> 0)
                    WHEN dRefuseDate_lc <= evDATE2 AND (iRefused_lc > 0 OR iToday_lc> 0)                      
                      THEN 1
                  ELSE 0  -- �������, ��� ������ ������ ������� � �����
                  END;    
       --------------  
       Return nRetval;              
    ELSE
      -- ����� ������ ������, ������� ��� �������� ��������...
      RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR, 'cd_cdch.592:ERROR: ��� �������� ���� <'
                            ||vcObjectType_loc||'> ����� ������� � ��������� ��������� <'||piruleMode
                            ||'> �� ������������. �������� � ������ ���������.'
                             );             
    END CASE;            
  End;        
  
Begin
    iLocalAttr := Coalesce(piruleMode,0);
    
    Return CASE iLocalAttr
             WHEN ci_Zero THEN rule_zero(pvcObjectType, pnObjID, TypeRep, evDATE)
             WHEN ci_One  THEN rule_one(pvcObjectType, pnObjID, TypeRep, evDATE)
             WHEN ci_Two  THEN rule_two(pvcObjectType, pnObjID, TypeRep, evDATE) 
               -- Vct 09.02.2016 z.160830
             WHEN ci_RULE_REQUEST THEN rule_REQUEST16(pvcObjectType, pnObjID, TypeRep, evDATE, ci_RecalcState) 
             -- Vct 01.11.2016
             -- WHEN ci_RULE_REQUEST THEN rule_REQUEST16(pvcObjectType, pnObjID, TypeRep, evDATE, ci_noRecalcState) 
             WHEN ci_RULE_REQUEST2 THEN rule_REQUEST16(pvcObjectType, pnObjID, TypeRep, evDATE, ci_noRecalcState)   -- Vct 15.08.2017             
               
             ELSE ci_Zero   
           END; 
End;                                
----------------------------------------------------------------------
-- Vct 28.06.2018 z.182475 �������� ������� ����������� ���� ��������� ���������� �������  ����� ��������� ����
Procedure DelAllRegisteredHistory(pn_agrID in Number -- ������������� ��������
                              , pd_dateStartFrom in Date
                              , pc_rpttype in Varchar2 -- ��� ������ TUTDF - 
                              )
is
 vc_rpttype cdch.ccdchtype%Type;
Begin
  vc_rpttype := Upper(pc_rpttype);
  -- ��������� �������� - cdch
  Delete From cdch t
  Where t.ncdchagrid = pn_agrID
  And t.dcdchdate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- ������ ��������    - cdch_rq, V_CDMO_Z_KB
  Delete From cdch_rq t
  Where 
  t.ncdchzid in (Select tt.IZID
                 From v_cdmo_z_kb tt where tt.NZAGRID = pn_agrID
                 )
  And t.dcdchdate >= pd_dateStartFrom -- Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- ����������� ��������� ������� -- cdch_d, v_cdh_doc_j
  Delete from cdch_d t
  Where 
    t.ncdchdid in (Select tt.ICDHID
                   From v_cdh_doc_j tt
                   Where tt.NCDHAGRID = pn_agrID
                  ) 
  And t.dcdchdate  >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  --  -  �������������� (�����������) cdch_czo, v_czo_kb_1
  Delete from cdch_czo  t
  Where 
  t.ncdchiczoid in (select tt.ICZO
                    from v_czo_kb_1 tt
                    where tt.NCZOAGRID = pn_agrID --= 2104 --:pp
                   )
  And t.ccdchtype = vc_rpttype
  And t.dcdchdate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  ;
  --   -- Vct 07.11.2019 - ������� ����������� ��������� PA
  Delete From CDCH_CLIPAY t
  Where t.ccdchtype = vc_rpttype
  And t.ncdchpa_objid =  pn_agrID
  And t.icdchpa_issccode = 27 -- �������
  And t.dcdch_paydate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  ;  
  -----
End;  
----------------------------------------------------------------------
----------------------------------------------------------------------
-- Vct 28.06.2018 z.182475 �������� ������� ����������� ���� ��������� ���������� �������  ����� ��������� ����
-- � ��������� ����������
Procedure DelAllRegisteredHistory_AT(pn_agrID in Number -- ������������� ��������
                                   , pd_dateStartFrom in Date
                                   , pc_rpttype in Varchar2 -- ��� ������ TUTDF - 
                                    )
is
  Pragma autonomous_transaction;
Begin
  ----------------------------------------------------------------------
  Begin
    DelAllRegisteredHistory(pn_agrID => pn_agrID -- ������������� ��������
                              , pd_dateStartFrom => pd_dateStartFrom
                              , pc_rpttype => pc_rpttype -- ��� ������ TUTDF - 
                              );
    Commit;
  Exception 
    When Others Then
      cd_utl2s.TxtOut('cd_cdch.DelAllRegisteredHistory_AT.823:ERROR:'||cd_errsupport.format_ora_errorstack(True));
      Rollback;
  End;
End;  
----------------------------------------------------------------------
-- Vct z.182475 
-- ��� ����������� � �������������� ����� ������� ��������
-- ���� ������ ��������� ������ - ������� ������� �������� �� ��������
-- � ���� ��������
-- ����� cd_cdch.ClearRegisteredHistory_svk(pn_agrId in Number)
Procedure ClearRegisteredHistory_svk(pn_agrId in Number)
  is
  ci_AgrClosed constant Integer := 3;
  cc_rpttype CONSTANT  cdch.ccdchtype%Type := 'TUTDF';
  
  vc_status cda_mf.ICDASTATUS%Type;
  vd_dcdaclosed Date;

Begin
  Begin
    Select cda.ICDASTATUS, cda.DCDACLOSED
    Into vc_status, vd_dcdaclosed
    From cda_mf cda 
    Where 
      cda.NCDAAGRID = pn_agrId;
    Exception 
      When No_data_Found Then
        Null;
  End;  
  IF vc_status IN (ci_AgrClosed)
    THEN
      DelAllRegisteredHistory_AT(pn_agrID => pn_agrId -- ������������� ��������
                               , pd_dateStartFrom => vd_dcdaclosed
                               , pc_rpttype => cc_rpttype -- ��� ������ TUTDF - 
                                );      
  END IF;  
End;    
----------------------------------------------------------------------
-- Vct 24.01.2019 z.190592 
-- �������� ������� ��������� � ������� ����������������� �������� � ���
-- ���������� 0, ���� �� ��������� � �������, 1 � �������� ������.
Function Document_Has_BKI_History(pn_docid in Number -- ������������� ���������
                                 ) return Number
is
--  ci_False constant pls_integer := 0;
  nret_val number := ci_False;
Begin
  Begin
    Select 1
    Into nret_val
    From v_cdch t
    where 1 = 1
      And t.cvcdchobjtype = 'CJDOCUMENT' 
      And t.nvcdchobj_id = pn_docid
      And rownum = 1  -- �� ������ ������������� ������� � �������
      ;
  Exception
      When no_data_found Then
        -- �������� �� ������ � �������, ���������� 0
        nret_val := ci_False;
  End;  
  Return nret_val;
End;  
----------------------------------------------------------------------
-- ������ ��������� ��������� �������� ��. ���������� � �������� �������� ��� �� ���
-- ��� ��������� ������� ������� ������� ����������� �������� ����������� ���������
Procedure Clear_Total_Doc_BKI_Hist(
                                  pn_docid_in in Number -- ������������� ���������
                                  )
is
Begin
  Delete from cdch_d t
  Where 
    t.ncdchdid = pn_docid_in
  ;
End;

----------------------------------------------------------------------
-- Vct 25.01.2019 ��� ������ � ����� cdhistdc.fmb
-- ������ ��������� ��������� �������� ��. ���������� � �������� �������� ��� �� ���
-- � ����� ������ ������������ �������������� ����������� ��������� ��� ����������� �������� � ���
-- ���� � ����� �������� ������� ������������ ����������,
-- �������� ����� �� ��������� ������������ ���� �������
Procedure mk_Document_BKI_Ready_cf(pn_docid_in in Number -- ������������� ���������
                                 , i_result_Out OUT Number -- ������
                                 , pc_error_message_out Out Varchar2
                                 )
is
-- TODO - ������ ������ � ���, ������ �� ���� ��� ����������� � ���������� ����������
-- (�� ������ 25.01.2019 � ����� cdhistdc.fmb ����� ������ �� OK, ����������� �����, ���� ������� ������ ����������� �������)
Begin
  Begin
    Clear_Total_Doc_BKI_Hist(
                            pn_docid_in => pn_docid_in -- ������������� ���������
                            );
                            
     i_result_Out := ci_SUccess;  
     pc_error_message_out := Null;    
  End;  
  -----
Exception 
  WHEN OTHERS THEN
    i_Result_out := ci_OTHER_ERROR;
    pc_error_message_out := cd_errsupport.format_ora_errorstack(True);  
End;
----------------------------------------------------------------------
begin
  -- Initialization
  Null;
end cd_cdch;
/
