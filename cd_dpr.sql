create or replace package cd_dpr authid Definer is

  -- Author  : VictorD
  -- Created : 2018-04-10 15:43:16
  -- Purpose : ������� ��� ������ ���� ��������� �������� �����������, ������������ � ��������� ��������� ��� ��������������� ����������� cde.

  -- Vct 10.04.2018 - ����� ������
  -- Vct 22.06.2018 - ���������� ������������ 409/410 ��������
  -- Vct 11.10.2018 {7.03.03}
  -- Vct 15.10.2018 {7.03.04} z. 1184692
  -- Vct 26.10.2018 {7.03.05} ����������� � ���������� ������ Print_Potok
  -- Vct 12.11.2018 {7.04.01}
  -- Vct 22.11.2018 {7.04.02} z.186064
  -- Vct 23.11.2018 {7.04.02} z.186064
  -- Vct 05.12.2018 {7.04.03} z.185823
  -- Vct 25.12.2018 {7.04.04} z.186861 ���������� ������� �����������, ������ ��� ������������ �������������
  -- Vct 29.12.2018 {7.04.05} mk_current_dpr, �������� PUT_LOG ��� ������ ������� � ��������� ��������
  -- Vct 01.02.2019 {7.04.06} mk_current_dpr, z.190837
  -- Vct 13.02.2019 {7.04.07} Put_Array_CDD, mk_initial_dpr z.190663
  -- Vct 14.02.2019 {7.04.08} ������ mk_current_dpr ��� ������  �� ������.
  -- Vct 19/02/2019 {7.04.09} �������� � mk_initial_dpr a_potok_xnpv.First --i_xnpv_pos --
  -- Vct 26.02.2019 {7.04.10} mk_current_dpr, z.191488 ������ ������� ���������� ������������ ������������� ��� ���������� ������� �����������
  -- Vct 27.02.2019 {7.04.11} mk_initial_dpr ������� 409/410 ��� ���������� ��������� ����������� ����� ������� ��������.
  -- Vct 05.03.2019 {7.04.12} mk_current_dpr,
  -- Vct 06.03.2019 {7.04.13} mk_current_dpr, z.191811
  -- Vct 10.03.2019 {7.04.14} mk_current_dpr,  z.192649
  -- Vct 06.06.2019 {7.05.01} mk_current_dpr_inner,mk_current_dpr z.193496
  -- Vct 15.07.2019 {7.05.02} mk_current_dpr_inner,mk_current_dpr z.194473
  -- Vct 19.07.2019 {7.05.03} Find_initial_Parts,   z.194593
  -- Vct 19.07.2019 {7.05.04}  z.194702, ��������� mk_current_dpr_bnk
  -- Vct 28.08.2019 (7.05.05) z.195326
  -- Vct 05.09.2019 (7.05.06) z.195326
  -- Vct 09.09.2019 (7.05.07) ���������� ������������� cd_psk_sutl.mk_datepoint_inflow
  -- Vct 03.10.2019 {7.05.08} z.195805 ����������� � mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 06.10.2019 {7.05.09} z.195805 ����������� � mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 09.10.2019 {7.05.10} Get_CliPay_ByPart + ...
  -- Vct 11.10.2019 {7.05.11} z.196254
  -- Vct 17.10.2019 {7.05.12} ... ������ � ������������ �������������� ������������... 17:39
  -- Vct 31.10.2019 {7.05.13} z.196758, mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 28.02.2020 {7.05.14} z.201209
  -- Vct 17.03.2020 {7.05.15} z.202252, 202298 - ���������� ��������� ��� ���������� ���������� 
  -- Vct 15.04.2020 {7.05.16} z.203025
  -- Vct 11.12.2020 {7.06.01} z.208543 
  -- Vct 08.04.2011 {7.06.02}  mk_current_dpr_bnk_fv
  ---------------------------------------
  -- 
  ---------------------------------------
  -- Public constant declarations
  Version CONSTANT VARCHAR2 (250) := ' $Id: {cd_dpr} {7.06.02} {2018.04.10/2021.04.08} Vct 17:21 $';

  -- record ��� ���������� ���������� ������ ����������� ������� � cde
  TYPE T_CDE_REGEVENT_CALLPRM_RT is Record(
      ncdeAgrid cde.ncdeagrid%Type      -- �������
    , icdePart cde.icdepart%Type        -- �����
    , icdeType cde.icdepart%Type        -- ��� �������
    , icdeSubType  cde.icdesubtype%Type -- ������ �������
    , dcdeDate cde.dcdedate%Type -- ���� ������
    , mcdeSum cde.mcdesum%Type  -- �����
    , ccdeRem cde.ccderem%Type  -- ����������� � ��������
    , ncdeCZO CDE.ncdeCZO%TYPE
    -- Vct 15.10.2018 - ������� - ����������� ������������� �������� ��� ���
    , cDeclarative CDD.CCDDNOTRN%TYPE -- Varchar2(2) -- Y - ������������, Null - � ���������
    , cCURISO cda.CCDACURISO%Type -- Vct 13.02.2019 - ������ ��� ����
--  TODO (����� ����)  , ncd4prority   CDD.icddprior_cd4%Type -- ������ ��������� �������� � cd4 ��� ������ �� ����� ��������� (��������� ������ �������� � cd4)
  );
  --------------------------------------------------------------------------------
  -- ������ ���������� ������� ��������� ����������� �������
  -- ��� ���������� cde � ���������� ��������� ��������� ������� �����������
  -- � ��������� ������������ ��������������� ������� � �������.
  Type T_CDE_CALL_QUEUE_PLTAB is table of T_CDE_REGEVENT_CALLPRM_RT Index by Pls_integer;
 -------
 isDBMS Boolean := True;
 -------------------------------------------------------------------
 -- ��������� ��� �������������  � cdgrp.Recalc_CDD_Item302
 -- ��� ���������� ���������� �������� '1051' - ������� �����������
 -- ������������ ���� ��� ������ ���� ��� - �� ��������� �� 10.04.2018
 Procedure mk_current_dpr( pn_agrId in Number -- �������
                         , pn_part in Integer -- ����� - ���� �� ����������...
                         , pd_evt_date in Date -- ����, � ������� ���������� ��������
                         , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + nocopy
                         --- z.196126 Vct 10.10.2019
                         -- �������� ������������� ��������� ������������� ������� �������� ����� ����������/������������ �������������
                         , pb_correction_required In Boolean Default False
                         ---
                        -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                         );
  -------------------------------------------------------------------
  -- ���������� ������ ������ � dbms_output
  -- ������������ � ���������� 26.10.2018
  Procedure Print_Potok(pc_prefix in Varchar2
                       , a_potok_xnpv in cd_types.t_pltab_potok2
                       , pn_isdbms In Integer --Default 0 -- 1 - ����������� dbms_output, 0 - �� �������� dbms_output
                       );
  -------------------------------------------------------------------
  -- Vct 22.11.2018 z.186064
  -- ��������� �������� ��������� (�� ���) �����������, � ����� ������ �������� ��������� ����������� ��������������
  -- ��� ���������� ���� ���������������� ������
  Function get_ac_discount_from_stage(pd_onDate in date) return Number;

  -------------------------------------------------------------------
  -- Vct 13.02.2019
  -- ���������� ������� ������������ �������� � cdd
 -- Vct 31.10.2019 - ����� ����������� OUT ��� ��������� a_result - ��� ������ � ��������� �� ����� �� ��������...
  Procedure Put_Array_CDD(a_result    IN /* OUT */ cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                        , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "���������", �������, ��� ������� ��� ������ �� cdgrp
                        , CDACURISO   CDA.CCDACURISO%TYPE -- ������ ��������, �������, ��� �������  ��� ������ �� cdgrp
                        , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output -- Vct 27.02.2019
                         );
  -------------------------------------------------------------------
  -- Vct 13.02.2019
  -- TODO - �������� �������� ��������� ��������� �� ����� �����
  -- ��� ������ ���� ���������� ����� � �������� ������ � ������. � �� ���������� ������� ���
  ---------------
  -- ��������� ����������� ��� ������ �� cdgrp.Recalc_CDD_Item302 ��� TypeMask in ('401','402')
  -- (������ ������� ������ cd_psk.get_XNPV � cdgrp)
  Procedure mk_initial_dpr( pn_agrId in Number -- �������
                          , pn_part in Integer -- ����� - ���� �� ����������...
                          , pd_evt_date in Date -- ����, � ������� ���������� ��������
                          , pn_calcstate In Number -- 1 - ��������� ������ ��������� � ���������, 0 - ���
                          , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                          , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "���������", �������, ��� ������� ��� ������ �� cdgrp
                          , CDACURISO   CDA.CCDACURISO%TYPE -- ������ ��������, �������, ��� �������  ��� ������ �� cdgrp
                          );
 -------------------------------------------------------------------
  -- ������� ���������� ����������   gr_initialdpr_parts
 Procedure Clear_initialdpr_parts;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
 -- Vct 04.06.2019
 -- ������� ���������� True, ���� �������� ��������� ������� ����������� ��� ������,
 -- �� ���� �� ����� ���� ������ ������ � ��������� ������������
 -- False ������������� ����� ��� ������������� - ����������� �� ���� ����� ������.
 Function isBankDeprecationSchema Return Boolean;
 -------------------------------------------------------------------
 -- Vct 14.10.2019 z.194874 ��������� ������� "���������� ���������" ��������
 -- = ������� �� ������� ����� ���� ������������ ���������� �� %% � ���������
 -- ��� ������������� � ����������� ������ 800707_152
 function get_balance_sheet_value(pn_agrID In Number -- ������������� ��������
                                , pd_onDate in Date -- ����, �� ������� ������ ���� �������� �������
                                  ) Return Number;
 -------------------------------------------------------------------
end cd_dpr;
/
create or replace package body cd_dpr is



  -- ���� ������������ ������
--  ci_SUCCESS constant Pls_integer :=0; -- �������� ����������
--  ci_NULLVALUE constant Pls_integer :=1; -- ����������� �������� ������ �����
--  ci_UNKNOWN_ERROR constant Pls_integer := 8192;  -- ������ ������ (����������� �������)
---------------------
 ci_One constant Pls_integer := 1;
 ci_Zero constant Pls_integer := 0;
 cn_Zero constant Number := 0.0;
-- ci_True constant pls_integer := 1;
 --
 cn_100 constant Number := 100.0;
 ci_One_Hundredth constant Number := 0.01;
 ------------------------------------
 -- �������, ��� ��� �������� ��������� ������ ���������� � 1
 cc_ParamValSetted constant cd0.ccd0value%Type := '1';
 ------------------------------------
 cn_CDBAL_ACC_AFTER constant Number := 520; -- ���� ����������� ����� ������ � cdbal
 --cn_CDBAL_ACC_REGISTRY_INCOME constant Number := 521; -- ���� �������� ������ �� �����������
 ----
 ci_ISDBMS constant Pls_integer := 1;
-------------------------------------------------------------------
-- Vct 15.10.2018
  cc_Declarative_event constant CDD.CCDDNOTRN%TYPE := 'Y';
 ------------------------------------------------------------------
/*
    -- , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                           -- , pa_parts_out OUT cd_types.T_PLDICT_NUMBER -- ������ ��������� ������
                            )
*/
 
  -- Vct 15.04.202 - ���� ����� ��������� � ������� ����������� ����������� ������������ ����������� �����������.
  -- ��� ��� ������� �����������
  TYPE T_REC_CURRENT_DPR_AGR Is Record(
     fn_agrID Number -- �������
   , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
   , fd_evt_date Date -- ����, � ������� ���������� ��������   
   , fb_recalc_state Boolean -- ������ - ��������� �������� �������� ��������� �� ��������
  );
  
 
  -- ��� ��� ��������� �����������
  -- ��� ��� ���������� ����������, ������ �������, ������ ������� ������������ �� �������� � ����� ������, ������� ������� ������������
  TYPE T_REC_INITIAL_DPR_AGR_CND Is Record(
      fn_agrID Number -- �������
    , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
    , a_parts cd_types.T_PLDICT_NUMBER -- ������ ������, ��� ������� ��������� �����������, ������ ������� ����� ������ �����
  );

  -- ������������ ��� ��������� �����������
  gr_initialdpr_parts T_REC_INITIAL_DPR_AGR_CND;

  -- Vct 15.05.2020
  -- ��� ������� �����������
  gr_currentdpr T_REC_CURRENT_DPR_AGR;
  
 ------ ��������� ��������� ---------------------------------------
 -- Vct 22.12.2018  �������������� ���������� �������� ���������� isDBMS ��� ������ � "������ job-�"
  Procedure Check_DBMSOUT_Job_Mode
    Is
  Begin
    -- ���� ������ - ��������� ����� � dbms_output
    IF CDENV.get_Job_DBMSOUT_Suppress_Mode()
      THEN
        isDBMS := False;
    END IF;
  End;
 -------------------------------------------
 Function Need_recalc_State_currentdpr Return Number
 Is 
 Begin
   Return case gr_currentdpr.fb_recalc_state
             when True Then 1
          else 0
          end;       
 End;     
 -------------------------------------------
 -- ������� ���� ��� ������� �����������
  Procedure Clear_currentdpr_cache
  Is
  -- ��� ������� �����������
  vr_currentdpr T_REC_CURRENT_DPR_AGR;
  Begin
    vr_currentdpr.fb_recalc_state := True;
    gr_currentdpr := vr_currentdpr;
  End;    
 -------------------------------------------
 -- ������� ���������� ����������   gr_initialdpr_parts
 Procedure Clear_initialdpr_parts
   is
   vr_initialdpr_parts T_REC_INITIAL_DPR_AGR_CND; -- ���������-�������� ��� ������� ���������� ����������

 Begin
   -- ��� ��� ���������� � cdgrp ����� ���������� ��������,
   -- ��������� � ��������� ������ ������ � ���������� ����� � ��� ������ ������ ��������
   Check_DBMSOUT_Job_Mode();
   ---
   gr_initialdpr_parts := vr_initialdpr_parts;
   
   -- Vct 15.05.2020
   -- � cdgrp ���� ����� Clear_initialdpr_parts
   -- ���� ��������� ������� ���������� ����������
   -- ��� ������� �����������
   -- TODO - ����� ���� ����� ��-���� ���������� ������ ��������� � ������� ����������� � ����� ����������
   Clear_currentdpr_cache();
   --
 End;
 ------------------------------------------------------------------
 -- ��������� ���������� ���������� ��� ������� �����������
 Procedure Setup_currentdpr_gchache(
                                   pn_agrID In Number -- �������
                                 , pc_dprcode In cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                                 , pd_evt_date In Date -- ����, � ������� ���������� ��������   
                                  )
   Is
   /*
  -- ��� ��� ������� �����������
  TYPE T_REC_CURRENT_DPR_AGR Is Record(
     fn_agrID Number -- �������
   , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
   , fd_evt_date Date -- ����, � ������� ���������� ��������   
   , fb_recalc_state Boolean -- ������ - ��������� �������� �������� ��������� �� ��������
  );   
   */
   vr_currentdpr T_REC_CURRENT_DPR_AGR;
   Procedure mk_new_dbp_cache(
                             pn_agrID In Number -- �������
                           , pc_dprcode In cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                           , pd_evt_date In Date -- ����, � ������� ���������� ��������   
                           , pr_newdpr OUT NOCOPY T_REC_CURRENT_DPR_AGR                           
                            )
   Is   
   Begin
     pr_newdpr.fn_agrID := pn_agrID;
     pr_newdpr.fc_dprcode := pc_dprcode;
     pr_newdpr.fd_evt_date := pd_evt_date;
     pr_newdpr.fb_recalc_state := True; -- ������������� ���������     
   End;  
 Begin
   vr_currentdpr := gr_currentdpr;

   IF cd_utl2s.is_equal(vr_currentdpr.fn_agrID, pn_agrID) 
     THEN -- ��� �� ����� �������
     -- ������������� ����� ��������� ���������         
     vr_currentdpr.fb_recalc_state := Case cd_utl2s.is_equal(vr_currentdpr.fd_evt_date, pd_evt_date)
                                      When True THEN
                                        -- ������ ������� � ���� ������� - ������� ���� ��������� ���������
                                        False
                                      Else
                                        -- ������������� ���� ��������� ���������
                                        True
                                     End;       
   ELSE
     -- ����� �������
     mk_new_dbp_cache(
                     pn_agrID => pn_agrID -- �������
                   , pc_dprcode => pc_dprcode -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                   , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������   
                   , pr_newdpr => vr_currentdpr
                    );
   END IF;    
   gr_currentdpr := vr_currentdpr;
 End;     
 ------------------------------------------------------------------
 -- ���������� � ������ -------------------------------------------
  Function to_money(pn_sum in Number) return Number
  is
  Begin
    Return Round(pn_sum,2);
  End;
  ---------------------------------------------------
  -- �������������� ���� ��� ������ � dbms_output
  Function fmt_date_out(pd in Date) Return Varchar2
  Is
  Begin
    Return to_char(pd,'DD.MM.YYYY');
  End;
  ---------------------------------------------------
  -- ������ � dbms-output
  Procedure db_out(pc_prefix in Varchar2, pc_text in Varchar2)
    Is
  Begin
    IF isDBMS THEN
      cd_utl2s.TxtOut((pc_prefix||':')||pc_text);
    END IF;
  End;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
 -- Vct 14.10.2019 z.194874 ��������� ������� "���������� ���������" ��������
 -- = ������� �� ������� ����� ���� ������������ ���������� �� %% � ���������
 -- ��� ������������� � ����������� ������ 800707_152
 -- ����� cd_dpr.get_balance_sheet_value(pn_agrID In Number, pd_onDate in Date)
 function get_balance_sheet_value(pn_agrID In Number -- ������������� ��������
                                , pd_onDate in Date -- ����, �� ������� ������ ���� �������� �������
                                  ) Return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- ������� �������������
   n_retval := Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                       , defdate_in => pd_onDate
                                       ,TYPEACC1_in => 1   -- ������� ������� ������������� ��� �������� ��������
                                       ,TYPEACC2_in => 701 -- ������� ������� ������������� ��� ������
                                       ), cn_Zero);
   -----
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 10  -- ���� ���������� �����
                                                 -- ,TYPEACC2_in => 101
                                                  ,TYPEACC2_in => 40 -- ���� ����� ����������� ��������
                                                  ), cn_Zero);
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 705 -- ���� ������������� ���� %%
                                                  ,TYPEACC2_in => 781 -- ���� ������������� ���� ��������
                                                  ),cn_Zero);
   Return n_retval;
 End;
 -------------------------------------------------------------------
 -- ����� ���������� �����
 function get_accrued_req(pn_agrID In Number -- ������������� ��������
                        , pd_onDate in Date -- ����, �� ������� ������ ���� �������� �������
                         ) return Number
 is
   n_retval Number:= cn_Zero;
 Begin
   n_retval := Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 10  -- ���� ���������� �����
                                                 -- ,TYPEACC2_in => 101
                                                  ,TYPEACC2_in => 40 -- ���� ����� ����������� ��������
                                                  ),cn_Zero);
   -------
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 705 -- ���� ������������� ���� %%
                                                  ,TYPEACC2_in => 781 -- ���� ������������� ���� ��������
                                                  ), cn_Zero);
    ---
    Return n_retval;
 End;
 -------------------------------------------------------------------
  -- ����� ���������� �����
 function get_accrued_req(pn_agrID In Number -- ������������� ��������
                        , pn_part In Number  -- �����
                        , pd_onDate in Date -- ����, �� ������� ������ ���� �������� �������
                         ) return Number
 is
   n_retval Number:= cn_Zero;
 Begin
   n_retval := n_retval + cdbalance.get_CurSaldo2a( agrID_in => pn_agrID
                                                  , defdate_in => pd_onDate
                                                  , PART_in => pn_part
                                                  , TYPEACC1_in => 10  -- ���� ���������� �����
                                                 -- ,TYPEACC2_in => 101
                                                  , TYPEACC2_in => 40 -- ���� ����� ����������� ��������
                                                  );
   -------
   n_retval := n_retval + cdbalance.get_CurSaldo2a( agrID_in => pn_agrID
                                                  , defdate_in => pd_onDate
                                                  , PART_in => pn_part
                                                  , TYPEACC1_in => 705 -- ���� ������������� ���� %%
                                                  , TYPEACC2_in => 781 -- ���� ������������� ���� ��������
                                                  );
    ---
    Return n_retval;
 End;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
  -- ���������� ������ ������ � dbms_output
  Procedure Print_Potok(pc_prefix in Varchar2
                       , a_potok_xnpv in cd_types.t_pltab_potok2
                       , pn_isdbms In Integer --Default 0 -- 1 - ����������� dbms_output, 0 - �� �������� dbms_output
                       )
  Is

  Begin
    IF pn_isdbms = ci_ISDBMS
      THEN
      db_out(pc_prefix => pc_prefix
           , pc_text => ' ������ ������:'
            );
      FOR jk IN 1..a_potok_xnpv.COUNT()
      LOOP
        db_out(pc_prefix => '     '
             , pc_text =>  ('a_potok_xnpv('||jk||').ddate := ' || fmt_date_out(a_potok_xnpv(jk).ddate))
                         ||('; a_potok_xnpv('||jk||').inum := ' || cd_utl2s.num_to_str_dot(a_potok_xnpv(jk).inum))
                         ||('; a_potok_xnpv('||jk||').proz := ' || cd_utl2s.num_to_str_dot(a_potok_xnpv(jk).proz)||';')

              );
      END LOOP;
    END IF;
  End;
 -------------------------------------------------------------------
 -- Vct 09.10.2019 - ����������� ����� ����������� ������� �� ���������
 -- ���� ������� �-��� � cdsytate2.Get_ClientPay_LPCCOM...
 Function Get_CliPay_ByPart(p_AGRID in Number  -- pn_agrId
                          , p_ipart in Number -- => pn_part  -- ����� �����
                          , p_DFrom in Date -- => (d_prevevt_date + ci_One)
                          , p_DTO  in Date -- => pd_evt_date
                          ) Return Number
 Is
 nRetVal Number := cn_Zero;
 Begin
   ---
   select
      SUM(e.cli_out) cli_out              -- ��������� �������� (������� ������� ��)
--      SUM(e.inp+e.cli_out_pc+e.cli_out_com) cli_out              -- ��������� �������� (������� ��, %, �������)
   into nretval
      from v_cde0 e
   where e.dog=p_AGRID
   and e.part =  p_ipart
   and e.dat between p_DFrom and p_DTO;

   Return coalesce(nretval, cn_zero);
 End;
 -------------------------------------------------------------------
 -- Vct 09.10.2019 - ����������� ����� ����������� ������� �� ���������
 -- ���� ������� �-��� � cdsytate2.Get_ClientPay_LPCCOM...
 Function Get_CliPay(p_AGRID in Number  -- pn_agrId
                    --      , p_ipart in Number -- => pn_part  -- ����� �����
                   , p_DFrom in Date -- => (d_prevevt_date + ci_One)
                   , p_DTO  in Date -- => pd_evt_date
                    ) Return Number
 Is
 nRetVal Number := cn_Zero;
 Begin
   ---
   select
      SUM(e.cli_out) cli_out              -- ��������� �������� (������� ������� ��)
--      SUM(e.inp+e.cli_out_pc+e.cli_out_com) cli_out              -- ��������� �������� (������� ��, %, �������)
   Into nretval
   from v_cde0 e
   where e.dog=p_AGRID
   and e.dat between p_DFrom and p_DTO;

   Return coalesce(nretval, cn_zero);
 End;
 -------------------------------------------------------------------
 -- Vct 17.10.2019 ��������� ����� ����������� ��������� � ������� ���������
 Function get_AccruedPercent(pn_agrId In Number -- �������
                            , pc_RT IN Varchar2 -- ��� ������� %%, ��� �������� �������� �����
                            , pd_onDate In Date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                            ) return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- �� ���� no_data_found ���� ��� group by � �������
    Select SUM(t.MCDIACCRUED)
    into n_retval
    From v_cdi t
    Where t.CCDIRT = pc_RT
    And t.NCDIAGRID = pn_agrId
    And pd_onDate Between t.DCDIFROM And t.DCDITO
    ;
    Return Coalesce(n_retval, cn_zero);
 End;
 --
 -- Vct 17.10.2019 ��������� ����� ����������� ��������� � ������� ��������� �� �����
 Function get_AccruedPercent(pn_agrId In Number -- �������
                            , pn_part In Number -- ���� �����
                            , pc_RT IN Varchar2 -- ��� ������� %%, ��� �������� �������� �����
                            , pd_onDate In Date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                            ) return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- �� ���� no_data_found ���� ��� group by � �������
    Select SUM(t.MCDIACCRUED)
    into n_retval
    From v_cdi t
    Where t.CCDIRT = pc_RT
    And t.NCDIAGRID = pn_agrId
    And t.ICDIPART = pn_part
    And pd_onDate Between t.DCDIFROM And t.DCDITO
    ;
    Return Coalesce(n_retval, cn_zero);
 End;
 -------------------------------------------------------------------
 -- ��������� ����� ������� ��� ������ � cde
 -- ��������� �� ���� ��� � ����� �������� �������, � ����� ��� ���������� ������� �������
 --
 Function bf_mk_evt_difference(pn_agrID in Number -- ��� ��������
                              , pi_part in Integer -- �����
                              , pd_onDate in Date -- ����, � ������� ���������� ���������
                              , pn_goalSum in Number
                              , pi_evt_goal in Integer
                              , pi_evt_anti in Integer
                              )
          Return Number
 Is
   ndb_sum Number;     -- ����� � cde, ��������������� pi_evt_goal
   ndb_antisum Number; -- ����� � cde, ��������������� pi_evt_anti
   nretval Number;     -- ���������
   Cursor crs_cde2r(pn_agrID in Number -- ��� ��������
                  , pi_part in Integer -- �����
                  , pd_onDate in Date -- ����, � ������� ���������� ���������
                  , pi_evt_goal in Integer
                  , pi_evt_anti in Integer
                  )
   Is
   Select
     SUM(Case When e.icdetype = pi_evt_goal
            Then e.mcdesum
         Else 0.0 End) as f_sum,  -- ����� �������� �������
     SUM(Case When e.icdetype = pi_evt_anti
            Then e.mcdesum
         Else 0.0 End) as f_antisum -- ����� ������� �������
   From cde e
   Where
     e.ncdeagrid = pn_agrID
   And e.icdepart = pi_part
   And e.dcdedate = pd_onDate
   And (e.icdetype = pi_evt_goal
        OR
        e.icdetype = pi_evt_anti
       )
  Group by e.dcdedate
   ;
 Begin
   -- ��������� ndb_sum � ndb_antisum
   --------------------------------
   -- ������������ open-fetch-close � ����� � ���������, ��� ������� � ����������� ������� �� ����� ��������������
   -- (��������� no_data_found)
   Open crs_cde2r(pn_agrID => pn_agrID -- ��� ��������
                , pi_part => pi_part -- �����
                , pd_onDate => trunc(pd_onDate) -- ����, � ������� ���������� ���������
                , pi_evt_goal => pi_evt_goal
                , pi_evt_anti => pi_evt_anti
                );
   Fetch crs_cde2r Into ndb_sum, ndb_antisum;
   Close crs_cde2r;
   --------------------------------
   nretval := (pn_goalSum - COALESCE(ndb_sum, cn_Zero)) + COALESCE(ndb_antisum, cn_Zero);
   return nretval;
 End;
 -------------------------------------------------------------------
 -- ���������� ��������� ������� ����������� ����� ��������������� ������ � �������
 Function calc_DPR_Revenue( pf_rate_in in Number -- ��������� %%� ������ (����� - ������ �����������)
                          , pd_startdate in Date -- ���� ������ ���������
                          , pd_enddate in Date   -- ���� ���������� ���������
                          , ppf_pmt in Number    -- �����, �� ��������� � ������� ����������� �����
                                                 -- �� �������� �������������, ���� �������� ���������� �������
                          ) Return Number
 is
   n_dpr_full_money Number;
 Begin

    ------------------
    n_dpr_full_money :=  cd_fcc_pkg.FV_N_Simple( pf_rate_in => pf_rate_in
                                         , pd_startdate => pd_startdate
                                         , pd_enddate => pd_enddate
                                         , ppf_pmt => ppf_pmt
                                         );
    ------------------
    n_dpr_full_money := to_money(n_dpr_full_money);
    -- ����� ������ �� ����������� (���� 521, ������� 400)
    Return  (n_dpr_full_money - to_money(ppf_pmt));
 End;
 -------------------------------------------------------------------
 -- Vct 22.11.2018 z.186064
 -- ��������� �������� ��������� (�� ���) �����������, � ����� ������ �������� ��������� ����������� ��������������
 -- ��� ���������� ���� ���������������� ������
 Function get_ac_discount_from_stage(pd_onDate in date) return Number
 Is
 -- ������������� ����� �����
  cc_SQLID constant ac.T_Defint := 'CD.materiality.deprecation.params.discount_ac';
  va_params ac.T_TabParameter;
  --
  c_val cd_types.TVCHAR2000;
  n_val Number;
 Begin
   Begin
       va_params := ac.T_TabParameter(
                                     cd_utl2s.num_to_str_dot(cd_chdutils.JDN(pd_onDate))
                                     );
       -- ������ ������, ���� ������� ����� ����� � ���� ���������� �������������
       -- �� ���� �������� �� �����, �� ������ ���� ��������������� � ������������ ����� (.)
       c_val := AC.Get_Value2( cDefint => cc_SQLID
                    , tabParam => va_params
                    , bPlsSafe => False
                     );
        n_val := cd_utl2s.str_to_num_dot(c_val);
   End;
   Return n_val;
 End;

 -------------------------------------------------------------------
 -- Vct 22.11.2018 - z.186064
 -- �����������  ����� ���������������� ������ � ������ ���������� � (��������� �� ����) ������� �� ����� �����������
 -- ������� ����� ������������ ��������������
 -- ���� ����������� �������������� �����������, �������� ���� ������ ���������� �� (1  - kr)
 Function calc_DPR_Revenue_IFRS( pn_agrID In Number -- �������, ��� �������� ���������� ����������� ������
                               , pd_evtDate in Date -- "���� �������" ��� "������� ����", ��������, ����� ������ ����� ��������� �  pd_enddate
                               , pf_rate_in in Number -- ��������� %%� ������ (����� - ������ �����������)
                               , pd_startdate in Date -- ���� ������ ���������
                               , pd_enddate in Date   -- ���� ���������� ���������
                               , ppf_pmt in Number    -- �����, �� ��������� � ������� ����������� �����
                                                      -- �� �������� �������������, ���� �������� ���������� �������
                               , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                               ) Return Number
 Is
   n_base_money Number; -- ��� ����������� ����, � ������� ������� �����
   kr Number; -- ����������� ��������������

   n_agr_stage Number; -- ������ �������������� ��������
   n_rule_stage Number; -- �������� ���������, �����������, � ����� ������ �������� ��������� ����������� ��������������
   --
   n_retval Number;

 Begin
   n_base_money := ppf_pmt;
   -- �������� ������� (����� ������) � ������� ������������ ���� �������� ��� ���� ������
   n_rule_stage := get_ac_discount_from_stage(pd_onDate => pd_evtDate);
   -- �������� ������ �������������� � ������ ��������������
   -- !! ��������� ����� ������� n_agr_stage � kr ���������� - null-����������
   cdutil_ifrs.Get_Agr_IFRS_Stg_ResRate(pAgrID => pn_agrID
                                       ,pDate  => pd_startdate
                                       ,pStg => n_agr_stage
                                       ,pRate => kr
                                       );
   IF pb_isdbms
     THEN
       cd_utl2s.TxtOut('cd_dpr.calc_DPR_Revenue_IFRS.217: n_rule_stage :='||n_rule_stage
                    ||' n_agr_stage :='||n_agr_stage
                    ||' kr:='||kr
                    ||' pn_agrID:='||pn_agrID
                    ||' pd_startdate:='||to_char(pd_startdate, 'DD.MM.YYYY')
                    ||' pd_enddate:='||to_char(pd_enddate,'DD.MM.YYYY')
                    ||' pf_rate_in :='||pf_rate_in
                    ||' ppf_pmt:='||ppf_pmt
                      );
   END IF;
   -- ���������� � %% ����������� ����� �� 100 (�������� �� 0.01)
   kr := ci_One_Hundredth * kr;
   --------------------
   --  ��� ����������� ���� ������������ ������� ����� ������������ ������������� ������
   IF n_agr_stage >= n_rule_stage
     THEN -- ��������� ������� �� ����������� ��������������
       n_base_money := (1.0 - kr) * n_base_money;
   END IF;
   IF pb_isdbms
     THEN
       cd_utl2s.TxtOut('cd_dpr.calc_DPR_Revenue_IFRS.424: n_base_money :='||n_base_money
                      );
   END IF;
   n_retval := calc_DPR_Revenue( pf_rate_in => pf_rate_in -- ��������� %%� ������ (����� - ������ �����������)
                          , pd_startdate => pd_startdate  -- ���� ������ ���������
                          , pd_enddate => pd_enddate      -- ���� ���������� ���������
                          , ppf_pmt => n_base_money       -- �����, �� ��������� � ������� ����������� �����
                                                          -- �� �������� �������������, ���� �������� ���������� �������
                          );

   Return n_retval;
 End;
  -----------------------------------------
  -- ��������� ������������ ���������� �������� ��������� �������
  -- ������� ��������� ������������� ��������� T_CDE_REGEVENT_CALLPRM_RT
  -- ��� ������������� � mk_current_dpr, mk_initial_dpr
  Procedure make_next_out_array_element(
                                        icurPos In Out Pls_integer
                                      , pa_evt_queue IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                                      , p_ncdeAgrid in cde.ncdeagrid%Type      -- �������
                                      , p_icdePart in cde.icdepart%Type        -- �����
                                      , p_icdeType in cde.icdepart%Type        -- ��� �������
                                      , p_icdeSubType in cde.icdesubtype%Type Default Null -- ������ �������
                                      , p_dcdeDate in cde.dcdedate%Type -- ���� ������
                                      , p_mcdeSum in cde.mcdesum%Type  -- �����
                                      , p_ccdeRem in cde.ccderem%Type Default null  -- ����������� � ��������
                                      , p_ncdeCZO in CDE.ncdeCZO%TYPE Default Null
                                      , pc_Declarative In CDD.CCDDNOTRN%TYPE Default cc_Declarative_event
                                       )
  Is
  Begin
    ------------------------------------------
    icurPos := icurPos + ci_One;
    pa_evt_queue(icurPos).ncdeAgrid := p_ncdeAgrid; -- �������
    pa_evt_queue(icurPos).icdePart  := p_icdePart;  -- �����
    pa_evt_queue(icurPos).icdeType  := p_icdeType;  -- ��� �������
    --
    pa_evt_queue(icurPos).icdeType  := p_icdeType;  -- ��� �������
    --
    pa_evt_queue(icurPos).icdeSubType := p_icdeSubType; -- ������ �������

    pa_evt_queue(icurPos).dcdeDate  := p_dcdeDate;  -- ���� �������
    pa_evt_queue(icurPos).mcdeSum   := p_mcdeSum;   -- �����
    pa_evt_queue(icurPos).ccdeRem   := p_ccdeRem;   -- ����������� � ��������
    pa_evt_queue(icurPos).ncdeCZO   := p_ncdeCZO;
    -- Vct 15.10.2018
    pa_evt_queue(icurPos).cDeclarative := pc_Declarative;
    ------------------------------------------

  End;
 -------------------------------------------------------------------
 -- Vct 12.02.2019
 -- ��� ������������� ��� ���������� ��������� ����������� �� ������:
 -- ������ ������ ������ ��� ������� ���������� ��������� ��������� �����������
 -- ������ ��������� ������� ������������� �����, ��� ������� ��������� ���������� ��������
 Procedure Find_initial_Parts(pn_agr_in in Number -- ����� ��������
                            , pd_on_date in Date -- ����, � ������� ����� ������������ ����� � ��������� "������"
                            , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                            , pa_parts_out OUT NOCOPY cd_types.T_PLDICT_NUMBER -- ������ ��������� ������
                            )
 Is
   a_temp cd_types.T_PLDICT_NUMBER;
   iPos Binary_Integer; -- ������� � a_temp
   i_part Binary_Integer;
 Begin
   pa_parts_out := a_temp; --
   -- �������� ������������� ������� �����������
   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => pc_dprcode)
     THEN
       -- ����������� �� �������� �� �������,
       -- � ���� �� ����� ���� "���������� ������"
       -- ���������� ��������
       -- pa_parts_out := a_temp; -- �������� �� if
       Return; -- ������ �������
   END IF;
   --------- ��������� ���������� ������� ------------
   Begin
     IF cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dprcode)
       THEN
       -- ������� ������ �� ���� ������
       Select e.icdepart
       Bulk collect into a_temp
       From cde e
       Where 1 = 1
         And e.ncdeagrid = pn_agr_in
         And e.dcdedate = Cast(pd_on_date as Date)
         And e.icdetype in (1,41,701) -- ������ �������, ������� ��������� �� ������
         -- TODO - ������ ������ ������ ����� ������������. ������ ����� ����������� ��� ��������
         -- ��������, �� ������� ���������� ������� �� 520 ����� ��� ���-�� ���...
       ;

     ELSE
       -- ������� ������ ������ �� ������ ����� ��� ������ ������ �� ��������
       Select e.icdepart
       Bulk collect into a_temp
       From cde e
       Where 1 = 1
         And e.ncdeagrid = pn_agr_in
         And e.dcdedate = Cast(pd_on_date as Date)
         And e.icdepart = 1
         And e.icdetype in (1 -- ������
                            ,41 -- ������ � ����������� z.194593
                           ,701 -- �������� ��� ������� ������
                            ) -- ������ �������, ������� ��������� �� ������
       ;

     END IF;
   End;
   ------- �������������� "�������� � ������" --------
   -- pa_parts_out ����� �����������, �� ��������, ������ �� Exists, ������� ������ ����� ���������
   Begin
     iPos := a_temp.First();
     WHILE iPos Is Not Null
     LOOP
       i_part := a_temp(iPos);
       pa_parts_out(i_part) := 0; -- ��������, �� �������� �������� ����� �������� �� ���������� ��������� �����
       --
       iPos := a_temp.Next(iPos);
     END LOOP;
   End;

 End;

 -------------------------------------------------------------------
 -- Vct 12.02.2019
 -- ��� ������������� ��� ���������� ��������� ����������� �� ������:
 -- ��� ������������ ���������� ���������� � �������������� ��������� � ������/�������, �� �������
 -- ���������� ��������� ��������� �����������.
 Function create_initial_parts_descr(pn_agrid in Number -- ����� ��������
                            , pd_on_date in Date -- ����, � ������� ����� ������������ ����� � ��������� "������"
                           -- , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                           -- , pa_parts_out OUT cd_types.T_PLDICT_NUMBER -- ������ ��������� ������
                            ) Return T_REC_INITIAL_DPR_AGR_CND
 is
   vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC;
   vr_agrdpr_parts T_REC_INITIAL_DPR_AGR_CND;
 Begin

--  Clear_initialdpr_parts;

  vr_agrdpr_parts.fn_agrID := pn_agrid;

   -- ������� ������������� ������� �����������
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);
   vr_agrdpr_parts.fc_dprcode := vc_dpr_code;

   Find_initial_Parts(pn_agr_in => pn_agrid -- ����� ��������
                    , pd_on_date => pd_on_date -- ����, � ������� ����� ������������ ����� � ��������� "������"
                    , pc_dprcode => vc_dpr_code -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                    , pa_parts_out => vr_agrdpr_parts.a_parts -- ������ ��������� ������
                    );

   --gr_initialdpr_parts := vr_agrdpr_parts;
   Return vr_agrdpr_parts;
 End;
 -------------------------------------------------------------------
 -- Vct 15.04.2020 
 -- ��� ������������� � ������� �����������
 -------------------------------------------------------------------
 -- Vct 12.02.2019 �������� ������������� ��������� �����
 Procedure bp_checkpart_for_initial_dpr( pn_agrid in Number
                                       , pn_part  in Number
                                       , pd_on_date in Date
                                       , pb_good_part OUT Boolean -- ��������� - True - ����� ������� ���������,
                                                                  -- False - ��� - ����� ��� ����� �����������, ��� ��� ��� ���� ���������� �����
                                      )
 Is
 Begin
   IF NOT cd_utl2s.is_equal( gr_initialdpr_parts.fn_agrID,pn_agrid)
     THEN
       gr_initialdpr_parts := create_initial_parts_descr(pn_agrid => pn_agrid -- ����� ��������
                                                       , pd_on_date => pd_on_date -- ����, � ������� ����� ������������ ����� � ��������� "������"
                                                        );
   END IF;
   -- �������� ������������� �����
   pb_good_part := gr_initialdpr_parts.a_parts.Exists(pn_part);
   -- ��������, ��� ����� ����� �� �������������� (�������� �� ������� ����� ����� ����)
   IF pb_good_part
     THEN
       pb_good_part := (gr_initialdpr_parts.a_parts(pn_part) = cn_Zero);
   END IF;
 End;
 -------------------------------------------------------------------
 Procedure mark_part_processed( pn_agrid in Number
                              , pn_part  in Number

                              )
  is
  Begin
    IF cd_utl2s.is_equal(gr_initialdpr_parts.fn_agrID, pn_agrid)
      THEN
      IF gr_initialdpr_parts.a_parts.Exists(pn_part)
        THEN
         gr_initialdpr_parts.a_parts(pn_part) := ci_One;
      END IF;
    ELSE
      Null; -- TODO - ��������� �� ������
    END IF;
  End;
 -------------------------------------------------------------------
 -- Vct 13.02.2019
 -- ���������� ������� ������������ �������� � cdd
 -- Vct 31.10.2019 - ����� ����������� OUT ��� ��������� a_result - ��� ������ � ��������� �� ����� �� ��������...
 Procedure Put_Array_CDD(a_result    IN /* OUT */ cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                       , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "���������", �������, ��� ������� ��� ������ �� cdgrp
                       , CDACURISO   CDA.CCDACURISO%TYPE -- ������ ��������, �������, ��� �������  ��� ������ �� cdgrp
                       , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output -- Vct 27.02.2019
                        )
   is
   iPos Pls_integer;
 Begin
   -----------------------------
   -- �������
   IF pb_isdbms
     THEN
     cd_utl2s.TxtOut('cd_dpr.put_Array_CDD.437: CDACURISO='||CDACURISO
                   ||' evPrior='||evPrior
                    );
   END IF;
   iPos := a_result.First;
--   IF iPos Is Not Null   THEN
       --For j in 1..a_result.Count
       While iPos IS Not Null
       Loop
         -- �������
         IF pb_isdbms
           THEN
         cd_utl2s.TxtOut('cd_dpr.put_Array_CDD.446: iPos='||iPos
                       ||' a_result(iPos).ncdeAgrid='||a_result(iPos).ncdeAgrid
                       ||' a_result(iPos).dcdeDate ='||a_result(iPos).dcdeDate
                       ||' a_result(iPos).icdePart='||a_result(iPos).icdePart
                       ||' a_result(iPos).icdeType='||a_result(iPos).icdeType
                       ||' COALESCE(a_result(iPos).cCURISO, CDACURISO)='||COALESCE(a_result(iPos).cCURISO, CDACURISO)
                       ||' a_result(iPos).mcdeSum='||a_result(iPos).mcdeSum
                       ||' a_result(iPos).cDeclarative='||a_result(iPos).cDeclarative
                        );
         END IF;

         CDGRP.Insert_CDD(evPrior
                   , a_result(iPos).ncdeAgrid --AgrID
                   , a_result(iPos).dcdeDate -- evDATE
                   , a_result(iPos).icdePart   -- PartNum
                   , a_result(iPos).icdeType  --'409'
                   , COALESCE(a_result(iPos).cCURISO, CDACURISO)
                   , a_result(iPos).mcdeSum  -- maxsum
                   , a_result(iPos).mcdeSum  -- maxsum
                   , a_result(iPos).icdeSubType -- SubTypeEv   CDD.icddSUBTYPE%TYPE DEFAULT NULL,
                   ,null,null,null,null,null
                   , a_result(iPos).cDeclarative -- Vct 15.10.2018 z.184692        --,'Y'
                   );

         iPos := a_result.Next(iPos);
       End Loop;
--   END IF;
 End;
 -------------------------------------------------------------------
  -----------------------------------------
  -- Vct 03.06.2019 - �������� �� mk_initial_dpr_inner_bnk
  -- ��������� ��������� ����� ����������� ����� ������
  -- �� ���� ���� �� mk_current_dpr
  -- ��� ������������� � mk_initial_dpr_inner_bnk, mk_initial_dpr_inner
  Procedure bp_initial_XNPV_lc( pn_agrId in Number -- �������
                      , pn_Part in Integer -- �����, ���� �� ������������
                      , pc_extflowsqlid in Varchar2 -- ������������� ����� ����� ��� ��������� ������
                      , pn_recalc_state in Pls_integer -- 1 - ������������� ���������, 0 - ���
                      , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                     -- , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                      , pi_dpr_pecrent Number -- ������ �����������
                      , pd_flow_date in Date -- ����, �� ������� ������� �������� �����
                      , pd_evt_date  in Date -- ���� �������� �������.
                      , pm_ovd_sum in Number -- ����� ���������, ������� ���������� ������ ��� �������
                      ---
                      , pb_isdbms in Boolean -- Vct 03.06.2019
                      , bWrite_to_grpLog in Boolean -- Vct 03.06.2019
                      ---
                      , pm_sum_after_mn OUT Number -- ����� ����������� ����� ������, �� �� ������ ����� 520 ����
                      , pm_sum_before_mn OUT Number -- ����� �� ������ � ������ ������, ��� ����� ����� ������������ �������������
                      )
  Is
    a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
    r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv
    ----
    vd_ovdDate_toclc Date; -- ����, � ������� ������� ��������� ���������
    -- i_ovd_pos Pls_integer; -- ������� � ������, � ������� ����� ������ ���������
    ---------------
    m_sum_before Number;
    m_sum_after Number;
    -- -- Vct 23.11.2018  z. 186064
    m_ovd_sum_lc Number := cn_Zero;
    --b_use_ovd_in_flow Boolean; -- ���� true - ��������� ��������� � ������, ����� ��� ���������
    ---------------
    --i_xnpv_pos Pls_integer:= 1; -- �������, � ������� ��������� �������.
                                -- ���� ������� ������ � 1, ����� �����������...
    ---------------
    b_bypart_amrt Boolean := False;
  Begin
      -------------------------------------------------------------

      -------------------------------------------------------------
      b_bypart_amrt := cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dprcode);
      -------------------------------------------------------------
      -- �������� ������� ����� ��� �������� �������� �������
      -- �������� ����� cd_psk.get_XNPV �� ������ ������ � �������

      IF b_bypart_amrt
        Then -- ����������� � ������� ������
        cd_psk.get_XNPV_array2_by_part( pn_agrid_in => pn_agrId -- �������
                              , pn_part => pn_part -- ����� �����
                              , pc_extflowsqlid => pc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                              , pd_ondate => pd_flow_date -- ����, �� ������� �������� ���������� �����.
                              , pi_recalcState => pn_recalc_state  --Default 1 -- (Vct 03.05.2018) �������� ��������� 1 - �������������, 0 - ���
                              ---- Vct 09.01.2019 z.190043
                             -- , pc_TypeCalcState In  Varchar2 Default 'T' -- �������� ������ �� ����
                                                                       -- 'R'; --�������� ������ �� ������� ����
                              ----
                              , pa_potok_out => a_potok_xnpv -- �������� �����
                                -- ��������� ��� ��������� ��������� ������ � �������� ������� pa_potok_out
                              , pr_search_struct => r_bsearch_result
                              );

      ELSE -- ����������� � ����� �� ��������
        -- TODO - ���������, ��������������� �� ��������� � ���� ������.
        cd_psk.get_XNPV_array2( pn_agrid_in => pn_agrId -- �������
                      , pc_extflowsqlid => pc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                      , pd_ondate => pd_flow_date -- pd_evt_date -- ����, �� ������� �������� ���������� �����.
                      , pi_recalcState => pn_recalc_state
                      , pa_potok_out => a_potok_xnpv -- �������� �����
                        -- ��������� ��� ��������� ��������� ������ � �������� ������� pa_potok_out
                      , pr_search_struct => r_bsearch_result -- cd_psk_sutl.T_SEARCHD_CACHE '���������������� ��� ������� ���������
                      );
      END IF;

      -- �������
      IF pb_isdbms THEN
        Print_Potok(pc_prefix => 'cd_dpr.bp_initial_XNPV_lc.341: pc_dprcode='||pc_dprcode||' pn_part='||pn_part
                    , a_potok_xnpv => a_potok_xnpv
                    , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                    );
      END IF;
      ---------------------------------------------------------
      -- �������� ������������ ���������� ������             --
      -- ����������� ������, ���� ����� �������� ����������� --
      ---------------------------------------------------------
      Begin
        cd_fcc_pkg.Check_Potok( a_potok_in => a_potok_xnpv);
      Exception
        WHEN cd_errsupport.e_DATA_CORRUPTED
          THEN
           -- ����� ������
           -- TODO - �������� ������
           -- ...
           db_out(pc_prefix => 'cd_dpr.bp_initial_XNPV_lc.357:'
                , pc_text => ' pn_agrId:='||cd_utl2s.num_to_str_dot( pn_agrId)
                           ||' pn_Part:='||pn_Part
                           ||' pc_dprcode='||pc_dprcode
                           ||' pd_flow_date:='||fmt_date_out(pd_flow_date)
                           ||' pc_extflowsqlid:='||pc_extflowsqlid
                           ||' : '||cd_errsupport.format_ora_errorstack(b_with_backtrace => True)
                 );
           ------------------
           Print_Potok(pc_prefix => 'cd_psk.bp_initial_XNPV_lc.366:'
            , a_potok_xnpv => a_potok_xnpv
            , pn_isdbms => 1 -- ���� ����� �������� ������. -- case pb_isdbms when True then ci_One Else ci_Zero End
            );
          -- TODO  ������������ ������
          -- ��������� � ���������� ��������
          -- CONTINUE;
      End;
      ---------------------------------------------------------
      /*
      -- ������� ������� ���� � �����
      -- TODO - ��������, ����� ����� ���� �� ������ ����������� ��������� ������...
      Begin

        -----------------------------------------------------------------
        -- ����� ������� � �������, ��������������� ������� ���� �������
        cd_psk_sutl.bin_find_date_pos(p_array => a_potok_xnpv -- ������������� �� ���� ����� ��� �������� �������� � null �������� � �����
                                    , pr_scache => r_bsearch_result  -- ��������� �� ���������� ����������� ������
                                    , pd_DefDate => pd_evt_date -- d_evtDate -- ����, ������� ������� ������������ � p_array
                                     );

        -- ����� � ������ ������� ������ (� �� ����� ��������� ������ ���� �������� ���������� ������)
        -- i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
        IF  r_bsearch_result.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
           OR (r_bsearch_result.iOldIndex != cd_psk_sutl.ci_SEARCH_FAILED
               And r_bsearch_result.i_exact != cd_psk_sutl.ci_MATCH_EXACT
              )
           THEN
             -- �� ������� � ������ ������ ���� �������� �������.
             -- ����� ��������� ������ � ������� ������ � ���� �������� �������
          --------------------------------------------

          -- ��������� � ����� ������ � ������� ������ �� ������� ����
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => a_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => pd_evt_date --d_evtDate -- ����������� ����
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- ����������� ����� (����)
                                         , pi_foundPos => i_xnpv_pos -- �������, � ������� ��������� �������.
                                         );

          --------------------------------------------
          IF  i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
            THEN -- ��� ��������� �������� - ���� �� ������ ��������
                 -- �� ������� ������� ��� �������
                 -- TODO - �������� ������
                 -- ...
                   db_out(pc_prefix => 'cd_dpr.bp_XNVP_lc.342:'
                        , pc_text => ' ��� ���� <'
                        ||fmt_date_out(pd_evt_date)
                        ||'> �� ������� ������� ��� ������� � �����. ��������� ������ � ��������� '
                        ||cd_utl2s.num_to_str_dot(pn_agrid)
                         );
                   --------------------------------------------
                   -- TODO - ��������� ������ ???          ----
                   -- ��������� � ���������� �������� ???  ----
                   -- CONTINUE; -- ???                     ----
                   --------------------------------------------
          END IF;
          -- ���������� ����� ������ �����
          r_bsearch_result.iOldIndex := i_xnpv_pos;
          r_bsearch_result.i_exact := cd_psk_sutl.ci_MATCH_EXACT;
          -- ���������� ������ ������ � ��������� ������
          r_bsearch_result.iArrayEnd := a_potok_xnpv.Last;
        END IF;
      End;
      --
      */
      ---------------------------------------------------------
      ---------- ���� ��������� �� �������, ���������� ����� �� ���������� � �����
--      IF pm_ovd_sum != cn_Zero
--        THEN
               -- Vct 23.11.2018
--        b_use_ovd_in_flow := cd_psk_sutl.Encapsulate_ovd_in_flow(pc_code_in => pc_odvclc_code);
        -- Vct 23.11.2018 - if
--        IF b_use_ovd_in_flow
--          THEN
--        -- �������� ���� ����� ��������� � ������
--        vd_ovdDate_toclc := cd_psk_sutl.get_ovd_date_byCode( pa_flow => a_potok_xnpv      -- �����, �� ������� ������������ ����
--                                                           , pc_code => pc_odvclc_code     -- ��������� ������� ���������� ����
--                                                           , pd_reper_date_in => pd_evt_date -- ������� ����
--                                                           );
        -- ������� � ������ ������ � �����, ��������������� ���� ����� ���������
--        cd_psk_sutl.mk_datepoint_inflow( pa_flow => a_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
--                                       , pd_Date_in => vd_ovdDate_toclc -- ����������� ����
--                                       , pn_isum => cd_fcc_pkg.cn_Zero -- ����������� ����� (����)
--                                       , pi_foundPos => i_ovd_pos -- �������, � ������� ��������� ������� ������ ��� ���������
--                                       );
        -- ���������� ������ ������ � ��������� ������
--        r_bsearch_result.iArrayEnd := a_potok_xnpv.Last;

--        ELSE         -- Vct 23.11.2018 - elsif

--          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- �� ��������� ��������� � ������
--          m_ovd_sum_lc := pm_ovd_sum;

--        END IF;

--      ELSE
        -- ���� ����� ��������� ������������� � �������������
        vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE;
--      END IF;

      --------------------------------------------
      ----
      --------------------------------------------
      -- ��������� ����������� ����� ������ �� ������� ������ ������� � ������� ����,
      -- � ������ ���������
      cd_fcc_pkg.p_xirr_dflow_inner(a_flow_in => a_potok_xnpv -- ����� ��� ������������
                                   , pd_rate_in => pi_dpr_pecrent -- ������ ������ %%
                                   , pd_reperdate => pd_evt_date --d_evtDate  -- ������� ���� �� ��������� � ������� ������� ���� �������
                                   , pi_from => a_potok_xnpv.First --i_xnpv_pos --r_bsearch_result.iOldIndex --i_xnpv_pos -- ��������� ������ ��� ������������
                                   , pi_to => a_potok_xnpv.Last --r_bsearch_result.iArrayEnd  -- ������ ������� �������   -- �������� ������ ��� ������������
                                   , pm_sum_before => m_sum_before -- ����� ������� pi_from
                                   , pm_sum_after => m_sum_after --n_dpr_correct  -- ����� �� ������� pi_from
                                 --  , pm_part_from OUT T_FCC_NUMBER -- (rem Vct 17.11.2017) Vct 13.11.2017 - ����� ����� �����������, ����������� � ������ pi_from
                                   -- ��� ����� ��������� ���� ��������� ��� ������, ��� ������ ������������ ����� (�����+...)
                                   , pi_NonNegative_in => 0 --1 --0 -- Vct 04.10.2017 ���� ������������� ���� 1 - �� ���������, 0 ����� ��� ����
--                    -- Vct 13.02.2018 ��������� ��������� ����� �������������� ����� � ��������� ���� (��� ����� ���������)
--                    -- ���� ������ �������������� � ������
                                  , pd_additional_date => vd_ovdDate_toclc -- ���� ����� ���������
                                  , pm_additional_sum =>  pm_ovd_sum --m_ovd_ondate      -- ����� ���������
                                  );
      -- rem Vct 23.11.2018
      -- pm_sum_after_mn := to_money(m_sum_after );
      -- Vct 23.11.2018 z.186064
      pm_sum_after_mn := to_money(m_sum_after + m_ovd_sum_lc);
      pm_sum_before_mn  := to_money(m_sum_before);
  End;
 -----------------------------------------
 -------------------------------------------------------------------
 -- Vct 10.02.2019
 -- ��������� ��������� �����������
 -- (������ �������� ������, �������� ����������� xirr � cdgrp)
 Procedure mk_initial_dpr_inner( pn_agrId in Number -- �������
                         , pn_part in Integer -- ����� - ���� �� ����������...
                         , pc_dpr_code IN cd_dpr_utl.T_DO_AMRTPROC -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                         , pd_evt_date in Date -- ����, � ������� ���������� ��������
                         , pn_calcstate In Number -- 1 - ��������� ������ ��������� � ���������, 0 - ���
                         , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + NOCOPY
                         )
 Is
   i_out_index pls_integer := ci_Zero; -- ������ �������� ��������� ������� pa_result
   i_evt_code cde.icdetype%Type;       -- ��� ������������� �������
   ------------------------
   i_dpr_pecrent Number; -- ������ �����������
  -- vd_dpr_enddate Date; -- ���� ���������� �����������, ���� �� ����������
   m_sum_after_mn Number; -- ����� ����������� ����� ������, �� �� ������ ����� 520 ����
   m_sum_before_mn Number; -- ����� �� ������ � ������ ������, ��� ����� ����� ������������ �������������
   m_evt_amount Number; -- ��� ����� ��������

  -------------------------------------
  -- vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- ������������� ����� ����� ��� ��������� �������� ������
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
  vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
  bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
  ------------------------------------------
  vc_message cd_types.T_MAXSTRING;        -- ����� ��� �������������� ���������
  -- vc_message_text cd_types.TErrorString;  -- ��� ������ ����������� ������
  ------------------------------------------
  ------------------------------------------
 Begin
   -- ������� ������� � �������� ��������
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   
   --------------------------
   ---- ��������� ������ � ���� ���������� �����������
   Begin

      -- �������� ������ �����������.

      i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- ����� �����
                                                 , EffDate => pd_evt_date
                                                 , pc_code => pc_dpr_code
                                                 )/cn_100;

      -- ������� ���� ���������� ����������� �� ��������, ���� �����������
      --vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);

      -- ������ �������:
      -- ���� ������ �� ������������ (��� ����������) ��� ����� ���� (����������� ����������)
      -- ��� ���� ���������� ����������� ������������ pd_evt_date,
      -- �� ��������� ������ ���������
      IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
     --   OR pd_evt_date >= vd_dpr_enddate
        THEN
          vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                       ||' pn_part='||pn_part
                       ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                       ||' pc_dpr_code='||pc_dpr_code
                       ||' �� ����������� ����� ����������� ';
                      -- ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                      -- ||' : ����������� ��������� ��� �� ����������';

          IF bWrite_to_grpLog
            THEN
            CDGRP.LOG_PUT('E', pn_agrid, 'cd_dpr.mk_initial_dpr_inner.772:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
          ELSE
            db_out('cd_dpr.mk_initial_dpr_inner.772:'
                   , vc_message
                  );
          END IF;
          RETURN;
      END IF;
      ------�������� ���������� ������  -----------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                   -- ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                    ||' pc_dpr_code='||pc_dpr_code
                    ||' pn_part='||pn_part;
      END IF;
      --------
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
         db_out('cd_dpr.mk_initial_dpr_inner.791:'
              , vc_message
               );
      ELSIF bWrite_to_grpLog
        THEN
          CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_initial_dpr_inner.791:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
    End;
  --------------------------------------------------------
  -- �������� ������������� ����� ����� ��� ��������� �������� ������,
  -- ���� ������, ����� �������������� "����������� �����"
  -- + �������� ������ ���������� ����, � ������� ����� ��������� ����� ���������
  Begin
      --------------------------------------------
      -- �������� �����, ��������������� ���������� ���������
      --------------------------------------------
      -----
      -- �������� ������������� ����� �����, ������������ ���������������� ������ ����������� ������
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      -- ��������� � ��������� ����������� �� ���������
      /* -----------------------
      -- �������� ��������� ������� ����� ��������� � ������.
      -- ���� ����� ������� �� ����� �� ���� ������ �� ��������.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
      */
  End;

   -- ��������� �������� ��� ������������ ������������� � ����� ����������� ����� ������
   Begin
     bp_initial_XNPV_lc( pn_agrId => pn_agrId -- �������
                      , pn_Part => pn_part -- �����, ���� �� ������������
                      , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                      , pn_recalc_state => pn_calcstate -- 1 - ������������� ���������, 0 - ���
                      , pc_dprcode => pc_dpr_code -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                     -- , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                      , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                      , pd_flow_date => pd_evt_date -- ����, �� ������� ������� �������� �����
                      , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                      , pm_ovd_sum => 0.0 -- ����� ���������, ������� ���������� ������ ��� �������
                      , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                      , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                      , pm_sum_after_mn => m_sum_after_mn -- ����� ����������� ����� ������, �� �� ������ ����� 520 ����
                      , pm_sum_before_mn => m_sum_before_mn -- ����� �� ������ � ������ ������, ��� ����� ����� ������������ �������������
                      );
     ----- ���������� ���� � �������
      m_sum_before_mn := to_money(m_sum_before_mn);
      m_sum_after_mn := to_money(m_sum_after_mn);

      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' �� ������ m_sum_before_mn :='||cd_utl2s.num_to_str_dot(m_sum_before_mn)
                   -- ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                    ||' ����� ������ m_sum_after_mn='||cd_utl2s.num_to_str_dot(m_sum_after_mn);
      END IF;
      --------
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
         db_out('cd_dpr.mk_initial_dpr_inner.900:'
              , vc_message
               );
      ELSIF bWrite_to_grpLog
        THEN
          CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_initial_dpr_inner.900:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;


   End;

   ----- ������������ ��������� �������
   Begin
     -- ������������ ����� ������������ ������������� ������� 401/402
     m_evt_amount := m_sum_before_mn;
     i_evt_code := Case
                    When m_evt_amount > 0 Then 401
                    When m_evt_amount < 0 Then 402
                    Else ci_Zero
                  End;
     -------------------------
     IF i_evt_code != ci_Zero
       THEN
       -- i_out_index := i_out_index + ci_One; -- rem Vct 27.02.2019 ���������� �������� ���� ������ make_next_out_array_element
       m_evt_amount := Abs(m_evt_amount);
       make_next_out_array_element(  icurPos => i_out_index
                                   , pa_evt_queue => pa_result
                                   , p_ncdeAgrid => pn_agrID      -- �������
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                   , p_icdeType => i_evt_code   -- ��� �������
                                   ----
                                   , p_icdeSubType => Null -- ������ ������� --
                                   ----
                                   , p_dcdeDate => pd_evt_date -- ���� �������
                                   , p_mcdeSum => m_evt_amount
                                   , p_ccdeRem => '����., ������������ �������������, ������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                                  || case  cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dpr_code)
                                                       when True then ' ����� '||pn_part
                                                     end
                                  , pc_Declarative => to_char(Null) -- ��� �� ������������� ��������. ��� ������������� ���������� 'Y'
                                  );

     END IF;
     -- ����������� ��������, ���������� ������� 520 ����� �� ������ pm_sum_after_mn
     m_evt_amount := m_sum_after_mn - m_sum_before_mn;
     i_evt_code := Case
                    When m_evt_amount > 0 Then 409
                    When m_evt_amount < 0 Then 410
                    Else ci_Zero
                  End;
     ----------------
     IF i_evt_code != ci_Zero
       THEN
       -- i_out_index := i_out_index + ci_One; -- Vct 27.02.2019 - ���������� ������� ���� ������ make_next_out_array_element
       m_evt_amount := Abs(m_evt_amount);

       make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  ------
                                   , p_icdeSubType => 0 -- ������ ������� -- Vct 27.02.2019 ����� ��������� ������� 520 ��� ��������� � ��������� �������������� (������ 0)
                                  ----
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => m_evt_amount
                                  , p_ccdeRem => '������������� ������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                                  || case  cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dpr_code)
                                                       when True then ' ����� '||pn_part
                                                     end
                                 );
     END IF;
   End;

  ------ ����� ����� �� ���������������, �������� ������ � ������������  ���������� ����������--------------------------------

 End;

-------------------------------------------------------------------
  -----------------------------------------
  -- ��������� ����������� ������� ����� �������
  Procedure get_modification_start_on_date( pn_agrid in Number -- ������������� ��������
                                        -- Vct 14.02.2019
                                        , pn_part in Integer -- ����� �����,
                                        , pc_dpr_code cd_dpr_utl.T_DO_AMRTPROC --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                                        --
                                        , pd_reperDate in Date -- ������� ����, �� ������� ���������� ����� �����������
                                        , pb_isdbms in Boolean -- True - ����������� dbms_output
                                        , pd_mdf_startDate_out OUT Date --���� ������ �������� ��
                                        )
  is
    -- ������� �����������
    cd_CDR_I_ZERO_DATE constant Date := Date '1901-01-01';
    ----------------------------------------
    a_mdf_hist cd_types.T_MODIFICATION_PLTAB;
    r_bsearch_mdf_hist cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_mdf_hist
    ----------------------------------------
  Begin
    --------------------------------------------
    -- ������ ���� ��������� �������� �� ��������� ������ ������� �����������.
    -- � ����������� ������� �������� �� ���� ����  � ���������� ������� ������� �����������.
    -- ��������, ����� ���� �� ������� �����, �������� ���������� ������.
    -- �� ��������� ������ ������� ��� ��������� ���������� �� ����� �����,
    -- ����, ����� �� ������� ����� �����, ������������ ����� � ������� ������� �����������
    -- TODO - ���-�� �������� ������ ��� ������ � �������...
    --------------------------------------------
    -- �������� �����, ��������������� ����������������� (������������ ��������/���������)
    -- rem Vct 14.02.2019
    -- cd_psk.bp_get_modification_history(pn_agrid, a_mdf_hist);
    --------------------------------------------

    cd_psk.get_modification_hist_bycode(pn_agrId => pn_agrid  -- ����� ��������
                                     , pn_part => pn_part -- ����� �����
                                     , pc_code => pc_dpr_code
                                     , pa_mdf_hist_out => a_mdf_hist
                                      );

    ------------------------------------
    ----- ��������� ��������� ��������� --------------------
    cd_psk_sutl.mk_new_bsearch_struct(pa_potok => a_mdf_hist
                                     ,pr_search_struct => r_bsearch_mdf_hist
                                     );
    -------------
    -- ������� --
    Begin
      IF pb_isdbms
        THEN
        ----------
        db_out( 'cd_dpr.get_modification_start_on_date.267:'
                , 'a_mdf_hist.Count= '||a_mdf_hist.Count
                );
        ----------
        FOR jk IN 1..a_mdf_hist.Count
        LOOP
          db_out( 'cd_dpr.get_modification_start_on_date.273:'
                , ' a_mdf_hist(jk).ncdaagrid:='||cd_utl2s.num_to_str_dot(a_mdf_hist(jk).ncdaagrid)
                ||' a_mdf_hist(jk).dmodification:='||fmt_date_out(a_mdf_hist(jk).dmodification)
                ||' a_mdf_hist(jk).dstopDate:='||fmt_date_out(a_mdf_hist(jk).dstopDate)
                );
        END LOOP;
      END IF;
    End;
    ------------
    -- ���� ������� ���� � ������� �����������
    -- � ��������� �������� ��������
    Begin
      cd_psk_sutl.bin_find_date_pos(p_array => a_mdf_hist
                                , pr_scache => r_bsearch_mdf_hist
                                , pd_DefDate => pd_reperDate);
      -- � ��������� �������� ��������
      IF (r_bsearch_mdf_hist.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
         -- AND r_bsearch_mdf_hist.i_exact := cd_psk_sutl.ci_MATCH_FAILED
          )
          OR r_bsearch_mdf_hist.iOldIndex IS NULL -- ��� �� ������ �������������
        THEN   -- ������� ����� ���� ������ �� �������� �� �����-�� ��������
        pd_mdf_startDate_out := cd_CDR_I_ZERO_DATE; --Date '1901-01-01';
      ELSE
        -- ��������� �����, ����� ���������� ��������, �� ������ ��� ��� �� ����������.
        pd_mdf_startDate_out := a_mdf_hist(r_bsearch_mdf_hist.iOldIndex).dmodification;
      END IF;
    End;
    ------------
  End;

 ---------------
 -- ��������� ����������� ��� ������ �� cdgrp.Recalc_CDD_Item302 ��� TypeMask in ('401','402')
 -- (������ ������� ������ cd_psk.get_XNPV � cdgrp)
 Procedure mk_initial_dpr( pn_agrId in Number -- �������
                         , pn_part in Integer -- ����� - ���� �� ����������...
                         , pd_evt_date in Date -- ����, � ������� ���������� ��������
                         , pn_calcstate In Number -- 1 - ��������� ������ ��������� � ���������, 0 - ���
                         , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                         , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "���������", �������, ��� ������� ��� ������ �� cdgrp
                         , CDACURISO   CDA.CCDACURISO%TYPE -- ������ ��������, �������, ��� �������  ��� ������ �� cdgrp
                         )
 Is
   a_result cd_dpr.T_CDE_CALL_QUEUE_PLTAB; -- ������ �������������� � cde �������� ��� ������� � cdd
   --
   vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
   ------------------------------------------
   cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
   vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
   bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
   ------------------------------------------
   vc_message cd_types.T_MAXSTRING;        -- ����� ��� �������������� ���������
   vc_message_text cd_types.TErrorString;  -- ��� ������ ����������� ������
   ------------------------------------------
   b_goodPart Boolean := False; --
 Begin
   -- �������� ������ ������ � ���������� ����� � ��� ������ ������ ��������
   Check_DBMSOUT_Job_Mode();
   
   -- ������� ������� � �������� ��������
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);

   -- ������� ������������� ������� ����������� ( � ������ ��������� ���������)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ������ �������, ���� �� ���������� ������� ������� ����������� �� ��������
      vc_message:='�� �������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����������� �� ������� (�� ��������� ������� ������� ����������� � �������� ��������)';
     IF bWrite_to_grpLog
       THEN -- �������� ��� ��������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_initial_dpr.980:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_initial_dpr.980'
              , pc_text => vc_message
              );
     END IF;
     Return;
   ELSE
       vc_message:=' pn_agrId='|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||' vc_dpr_code='||vc_dpr_code;
        db_out(pc_prefix => 'cd_dpr.mk_initial_dpr.998'
              , pc_text => vc_message
              );
   END IF;
   ---------------------------
   -- �������� ������������� ������� ����������� �� �����
   bp_checkpart_for_initial_dpr( pn_agrid => pn_agrid
                               , pn_part  => pn_part
                               , pd_on_date => pd_evt_date
                               , pb_good_part => b_goodPart -- ��������� - True - ����� ������� ���������,
                                                          -- False - ��� - ����� ��� ����� �����������, ��� ��� ��� ���� ���������� �����
                              );

   -- ��������� �����������, ���� ����� �������� ������
   IF b_goodPart
     THEN
     -- ������ ����� ��������
     mk_initial_dpr_inner( pn_agrId => pn_agrId -- �������
                         , pn_part => pn_part -- ����� - ���� �� ����������...
                         , pc_dpr_code => vc_dpr_code -- ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                         , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������
                         , pn_calcstate => pn_calcstate -- 1 - ��������� ������ ��������� � ���������, 0 - ���
                         , pb_isdbms => pb_isdbms -- ������� ������ ���������� � dbms_output
                         , pa_result => a_result
                           );

     -- ���������� ���������� � cdd
     Put_Array_CDD(a_result    => a_result
                 , evPrior     => evPrior    -- "���������", �������, ��� ������� ��� ������ �� cdgrp
                 , CDACURISO   => CDACURISO -- ������ ��������, �������, ��� �������  ��� ������ �� cdgrp
                 , pb_isdbms => pb_isdbms -- ������� ������ ���������� � dbms_output -- Vct 27.02.2019
                  );
     mark_part_processed( pn_agrid => pn_agrId
                        , pn_part  => pn_part
                         );

   END IF;
 EXCEPTION
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - ��� ����� ���� �� ������������ ��� ���������� ����������� ������ ������ � �����-�� "��������� �������"
                                 -- ���� �� ����������...
      vc_message_text := SUBSTR(cd_errsupport.format_ora_errorstack(True),1,2000);
      vc_message_text := substr('cd_dpr.mk_initial_dpr:ERROR:(pn_agrId='||pn_agrId
                             ||' pn_part='||pn_part
                             ||' pd_evt_date='||to_char(pd_evt_date,'DD.MM.YYYY')
                             ||'):'
                             ||vc_message_text, 1,2000);

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_initial_dpr.1035:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
          -- TODO: ��������, ��� ����������� ������, ��������� ������� ��� ������ �� ������������ ������.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- �������� ��� ������...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;
 End;
 -------------------------------------------------------------------
  -----------------------------------------
  -- Vct 03.06.2019 - �������� �� ���� mk_current_dpr_inner
  -- ��������� ��������� ����� ����������� ����� ������
  -- ��� ������������� � ��������� ������� �����������
  Procedure bp_XNPV_lc( pn_agrId in Number -- �������
                      -- Vct 14.02.2019
                      , pn_part in Integer -- ����� �����,
                      , pc_dpr_code cd_dpr_utl.T_DO_AMRTPROC --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                      --
                      , pc_extflowsqlid in Varchar2 -- ������������� ����� ����� ��� ��������� ������
                      , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                      , pi_dpr_pecrent Number -- ������ �����������
                      , pd_flow_date in Date -- ����, �� ������� ������� �������� �����
                      , pd_evt_date  in Date -- ���� �������� �������.
                      , pm_ovd_sum in Number -- ����� ���������, ������� ���������� ������ ��� �������
                      ---
                      , pb_isdbms in Boolean -- Vct 03.06.2019
                      , bWrite_to_grpLog in Boolean -- Vct 03.06.2019
                      ---
                      , pm_sum_after_mn OUT Number
                      , pa_potok_xnpv IN OUT NOCOPY  cd_types.t_pltab_potok2 -- ������� ����� ��� ������ ����������� ����� ������
                      -- Vct 28.08.2019 - ������� ���������� ������� ���� (pd_evt_date) � �����
                      , pb_flow_modified OUT Boolean
                      -- Vct 23.09.2019
                      , i_evtdate_out_pos OUT Pls_integer -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
                      )
  Is

    r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv
    ----
    vd_ovdDate_toclc Date; -- ����, � ������� ������� ��������� ���������
    i_ovd_pos Pls_integer; -- ������� � ������, � ������� ���� ������ ���������
    ---------------
    m_sum_before Number;
    m_sum_after Number;
    -- -- Vct 23.11.2018  z. 186064
    m_ovd_sum_lc Number := cn_Zero;
    b_use_ovd_in_flow Boolean; -- ���� true - ��������� ��������� � ������, ����� ��� ���������
    ---------------
    i_xnpv_pos Pls_integer; -- �������, � ������� ��������� �������.
    ---------------
    b_ovd_flow_modified Boolean := False;
  Begin
      pb_flow_modified := False;

      -- TODO - ?�������� ���� ������ ���������? -
      -------------------------------------------------------------
      -- �������� ������� ����� ��� �������� �������� �������
      -- �������� ����� cd_psk.get_XNPV �� ������ ������ � �������

      IF cd_dpr_utl.bf_DeprecateByPart_Code(pc_dpr_code)
      THEN
        cd_psk.get_XNPV_array2_by_part( pn_agrid_in => pn_agrId -- �������
                                , pn_part => pn_part -- ����� �����
                                , pc_extflowsqlid => pc_extflowsqlid  -- ������������� ����� ����� ��� ��������� ������
                                , pd_ondate => pd_flow_date -- ����, �� ������� �������� ���������� �����.
                                -- TODO - ���������� ��������� !!!
                                -- z.203025
                                , pi_recalcState => Need_recalc_State_currentdpr() --In Pls_integer Default 1 -- (Vct 03.05.2018) �������� ��������� 1 - �������������, 0 - ���
                                ---- Vct 09.01.2019 z.190043
                                -- Vct 28.02.2020 z.201909
                                , pc_TypeCalcState => 'R' -- Varchar2 Default 'T' -- �������� ������ �� ����
                                                          -- 'R'; --�������� ������ �� ������� ����
                                ----
                                , pa_potok_out => pa_potok_xnpv -- �������� �����
                                  -- ��������� ��� ��������� ��������� ������ � �������� ������� pa_potok_out
                                , pr_search_struct => r_bsearch_result
                                -- Vct 17.03.2020 ������ ��� ���������� ����������
                                , pn_auto_transaction => 0
                                );
      -------------------------------------------------------------
      ELSE
        -- TODO - ���������, ��������������� �� ��������� � ���� ������.
        -- TODO-2 ������ (�� ��������� �� 12.04.2018) �� ��� �� �������, ��� ���������� � �������...
        cd_psk.get_XNPV_array2( pn_agrid_in => pn_agrId -- �������
                              , pc_extflowsqlid => pc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                              , pd_ondate => pd_flow_date -- pd_evt_date -- ����, �� ������� �������� ���������� �����.
                              -- z.203025
                              ,pi_recalcState => Need_recalc_State_currentdpr()
                              , pa_potok_out => pa_potok_xnpv -- �������� �����
                              -- ��������� ��� ��������� ��������� ������ � �������� ������� pa_potok_out
                              , pr_search_struct => r_bsearch_result -- '���������������� ��� ������� ���������
                              -- ��, ��� �����������...
                              -- Vct 28.02.2020 z.201909
                              , pc_TypeCalcState => 'R' -- 'T' - �������� ������ �� ����
                              -- Vct 17.03.2020 ������ ��� ���������� ����������
                              , pn_auto_transaction => 0                              
                              );
      END IF;
      ----------------------------------------------------------
      -- �������
      ----------------------------------------------------------
      IF pb_isdbms
        THEN
        Print_Potok(pc_prefix => 'cd_dpr.bp_XNPV_lc.264:'
                    , a_potok_xnpv => pa_potok_xnpv
                    , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                    );
      END IF;
      ---------------------------------------------------------
      -- �������� ������������ ���������� ������             --
      -- ����������� ������, ���� ����� �������� ����������� --
      ---------------------------------------------------------
      Begin
        cd_fcc_pkg.Check_Potok( a_potok_in => pa_potok_xnpv);
      Exception
        WHEN cd_errsupport.e_DATA_CORRUPTED
          THEN
           -- ����� ������
           -- TODO - �������� ������
           -- ...
           db_out(pc_prefix => 'cd_dpr.bp_XNPV_lc.281:'
                , pc_text => ' pn_agrId:='||cd_utl2s.num_to_str_dot( pn_agrId)
                          -- ||' pn_Part:='||pn_Part
                           ||' pd_flow_date:='||fmt_date_out(pd_flow_date)
                           ||' pc_extflowsqlid:='||pc_extflowsqlid
                           ||' : '||cd_errsupport.format_ora_errorstack(b_with_backtrace => True)
                 );
           ------------------
           Print_Potok(pc_prefix => 'cd_psk.bp_XNPV_lc.290:'
            , a_potok_xnpv => pa_potok_xnpv
            , pn_isdbms => 1 -- ���� ����� �������� ������. -- case pb_isdbms when True then ci_One Else ci_Zero End
            );
          -- TODO  ������������ ������
          -- ��������� � ���������� ��������
          -- CONTINUE;
      End;
      ---------------------------------------------------------
      -- ������� ������� ���� � �����
      -- TODO - ��������, ����� ����� ���� �� ������ ����������� ��������� ������...
      Begin

        /* rem Vct 09.09.2019 - �������� ��� (�������� ����� ������������� ������)...
        -----------------------------------------------------------------
        -- ����� ������� � �������, ��������������� ������� ���� �������
        cd_psk_sutl.bin_find_date_pos(p_array => pa_potok_xnpv -- ������������� �� ���� ����� ��� �������� �������� � null �������� � �����
                                    , pr_scache => r_bsearch_result  -- ��������� �� ���������� ����������� ������
                                    , pd_DefDate => pd_evt_date -- d_evtDate -- ����, ������� ������� ������������ � p_array
                                     );

        -- ����� � ������ ������� ������ (� �� ����� ��������� ������ ���� �������� ���������� ������)
        -- i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
        IF  r_bsearch_result.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
           OR (r_bsearch_result.iOldIndex != cd_psk_sutl.ci_SEARCH_FAILED
               And r_bsearch_result.i_exact != cd_psk_sutl.ci_MATCH_EXACT
              )
           THEN
             -- �� ������� � ������ ������ ���� �������� �������.
             -- ����� ��������� ������ � ������� ������ � ���� �������� �������
          -- pb_flow_modified := True; -- rem Vct 09.09.2019
            --------------------------------------------

          -- ��������� � ����� ������ � ������� ������ �� ������� ����
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => pd_evt_date --d_evtDate -- ����������� ����
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- ����������� ����� (����)
                                         , pi_foundPos => i_xnpv_pos -- �������, � ������� ��������� �������.
                                         -- Vct 09.09.2019
                                         , pb_row_inserted => pb_flow_modified
                                         );

          --------------------------------------------
          IF  i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
            THEN -- ��� ��������� �������� - ���� �� ������ ��������
                 -- �� ������� ������� ��� �������
                 -- TODO - �������� ������
                 -- ...
                   db_out(pc_prefix => 'cd_dpr.bp_XNVP_lc.342:'
                        , pc_text => ' ��� ���� <'
                        ||fmt_date_out(pd_evt_date)
                        ||'> �� ������� ������� ��� ������� � �����. ��������� ������ � ��������� '
                        ||cd_utl2s.num_to_str_dot(pn_agrid)
                         );
                   --------------------------------------------
                   -- TODO - ��������� ������ ???          ----
                   -- ��������� � ���������� �������� ???  ----
                   -- CONTINUE; -- ???                     ----
                   --------------------------------------------
          END IF;
          -- ���������� ����� ������ �����
          r_bsearch_result.iOldIndex := i_xnpv_pos;
          r_bsearch_result.i_exact := cd_psk_sutl.ci_MATCH_EXACT;
          -- ���������� ������ ������ � ��������� ������
          r_bsearch_result.iArrayEnd := pa_potok_xnpv.Last;
        END IF;
        */

        -- �������
        -- TODO - ������.
        IF pb_isdbms   THEN
          cd_utl2s.TxtOut('cd_dpr.1368.bp_XNPV_LC:cd_psk_sutl.mk_datepoint_inflow before: pa_potok_xnpv.First='
                        ||pa_potok_xnpv.First||' pa_potok_xnpv.Last='||pa_potok_xnpv.Last); -- pa_potok_xnpv
        END IF;
        --------------------------------------------------------------------------
        -- Vct 09.09.2019 - �������� ��� (��������� ������� �������� �����...) ...
        --
        -- ��������� � ����� ������ � ������� ������ �� ������� ����
        cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                       , pd_Date_in => pd_evt_date --d_evtDate -- ����������� ����
                                       , pn_isum => cd_fcc_pkg.cn_Zero -- ����������� ����� (����)
                                       , pi_foundPos => i_xnpv_pos -- �������, � ������� ��������� �������.
                                       -- Vct 09.09.2019
                                       , pb_row_inserted => pb_flow_modified
                                       );
         i_evtdate_out_pos := i_xnpv_pos;

       IF pb_isdbms   THEN
        cd_utl2s.TxtOut('cd_dpr.1383.bp_XNPV_LC:after: pa_potok_xnpv.First='
                      || pa_potok_xnpv.First||' pa_potok_xnpv.Last='||pa_potok_xnpv.Last
                      ||' i_xnpv_pos='||i_xnpv_pos
                      ||' pb_flow_modified='||case When pb_flow_modified = true Then 'True'
                                                   When pb_flow_modified = False Then 'False'
                                                   Else 'Null(!)'
                                              end
                      ); -- pa_potok_xnpv
        Print_Potok(pc_prefix => 'cd_dpr.bp_XNPV_lc.1391 after insert:'
                    , a_potok_xnpv => pa_potok_xnpv
                    , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                    );
       END IF;
      End;
      --
      ---------------------------------------------------------
      ---------- ���� ��������� �� �������, ���������� ����� �� ���������� � �����
      IF pm_ovd_sum != cn_Zero
        THEN
        -- Vct 23.11.2018
        b_use_ovd_in_flow := cd_psk_sutl.Encapsulate_ovd_in_flow(pc_code_in => pc_odvclc_code);
        -- Vct 23.11.2018 - if
        IF b_use_ovd_in_flow
          THEN
          -- �������� ���� ����� ��������� � ������
          vd_ovdDate_toclc := cd_psk_sutl.get_ovd_date_byCode( pa_flow => pa_potok_xnpv      -- �����, �� ������� ������������ ����
                                                             , pc_code => pc_odvclc_code     -- ��������� ������� ���������� ����
                                                             , pd_reper_date_in => pd_evt_date -- ������� ����
                                                             );

          -- ������� � ������ ������ � �����, ��������������� ���� ����� ���������
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => vd_ovdDate_toclc -- ����������� ����
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- ����������� ����� (����)
                                         , pi_foundPos => i_ovd_pos -- �������, � ������� ��������� ������� ������ ��� ���������
                                         -- Vct 09.09.2019
                                         , pb_row_inserted => b_ovd_flow_modified
                                         );
          -- ���������� ������ ������ � ��������� ������
          --r_bsearch_result.iArrayEnd := pa_potok_xnpv.Last; -- rem Vct 04.10.2019 - ��� ����� ��������

        ELSIF cd_psk_sutl.DoNotUse_ovd_in_Flow(pc_code_in =>  pc_odvclc_code)
          THEN -- z.191811 - �� ���������� ��������� � �������

          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- �� ��������� ��������� � ������
          m_ovd_sum_lc := cn_Zero;

        ELSE         -- Vct 23.11.2018 - elsif

          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- �� ��������� ��������� � ������
          m_ovd_sum_lc := pm_ovd_sum;

        END IF;

      ELSE
        -- ���� ����� ��������� ������������� � �������������
        vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE;
      END IF;
      --------------------------------------------
      ----
      --------------------------------------------
      IF pb_isdbms
        THEN
        Print_Potok(pc_prefix => 'cd_dpr.bp_XNPV_lc.1443 cd_fcc_pkg.p_xirr_dflow_inner:'
                      , a_potok_xnpv => pa_potok_xnpv
                      , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                      );
      END IF;
      -- ��������� ����������� ����� ������ �� ������� ������ ������� � ������� ����,
      -- � ������ ���������
      cd_fcc_pkg.p_xirr_dflow_inner(a_flow_in => pa_potok_xnpv -- ����� ��� ������������
                                   , pd_rate_in => pi_dpr_pecrent -- ������ ������ %%
                                   , pd_reperdate => pd_evt_date --d_evtDate  -- ������� ���� �� ��������� � ������� ������� ���� �������
                                   -- rem Vct 04.10.2019 -- �������, ����� ���� ������...
                                   , pi_from => i_evtdate_out_pos -- Vct 04.10.2019
                                   --, pi_from => r_bsearch_result.iOldIndex --i_xnpv_pos -- ��������� ������ ��� ������������
                                   , pi_to => pa_potok_xnpv.Last --r_bsearch_result.iArrayEnd  -- ������ ������� �������   -- �������� ������ ��� ������������
                                   , pm_sum_before => m_sum_before -- ����� ������� pi_from
                                   , pm_sum_after => m_sum_after --n_dpr_correct  -- ����� �� ������� pi_from
                                 --  , pm_part_from OUT T_FCC_NUMBER -- (rem Vct 17.11.2017) Vct 13.11.2017 - ����� ����� �����������, ����������� � ������ pi_from
                                   -- ��� ����� ��������� ���� ��������� ��� ������, ��� ������ ������������ ����� (�����+...)
                                   , pi_NonNegative_in => 1 --0 -- Vct 04.10.2017 ���� ������������� ���� 1 - �� ���������, 0 ����� ��� ����
--                    -- Vct 13.02.2018 ��������� ��������� ����� �������������� ����� � ��������� ���� (��� ����� ���������)
--                    -- ���� ������ �������������� � ������
                                  , pd_additional_date => vd_ovdDate_toclc -- ���� ����� ���������
                                  , pm_additional_sum =>  pm_ovd_sum --m_ovd_ondate      -- ����� ���������
                                  );
      -- rem Vct 23.11.2018
      -- pm_sum_after_mn := to_money(m_sum_after );
      -- Vct 23.11.2018 z.186064
      pm_sum_after_mn := to_money(m_sum_after + m_ovd_sum_lc);

  End;

 ------------------------------------------------------------------
 -- Vct 04.06.2019
 -- ������� ���������� True, ���� �������� ��������� ������� ����������� ��� ������,
 -- �� ���� �� ����� ���� ������ ������ � ��������� ������������
 -- False ������������� ����� ��� ������������� - ����������� �� ���� ����� ������.
 Function isBankDeprecationSchema Return Boolean
 is
  -- cc_DPRREVENUEBANKSCHEMA constant cd0.ccd0value%Type := '1';
   vc_param cd0.ccd0value%Type;
 Begin
   -- �������� ����������� ���������.
   vc_param := cdState.Get_CD0_Params(109);
   Return Coalesce((vc_param = cc_ParamValSetted /*cc_DPRREVENUEBANKSCHEMA*/) ,False);
 End;
 --------------------------------------------------------------------
 -- ��������� �������� ������� 520 ����� �� ��������� ���������������� ������
 -- ��� �� �������� XNPV (������)
 Function Calc520_From_RecordedIncome Return Boolean 
 is
   vc_param cd0.ccd0value%Type;
 Begin 
   vc_param := cdState.Get_CD0_Params(123); -- '0' - ������ �� ������, '1' - ������ �� �������� �� 521 ����� (�������� ���������������� �����)
   Return Coalesce((vc_param = cc_ParamValSetted) ,False);
 End;  
---------------------------------------------------------------------
-- Vct 03.10.2019
-- ��������� ����� ���������� ������������� �� ������
-- � ����� �� �������� ��� ������� �� ����� ����� � ����� �� ��������
  Function get_cdeDprPrcSubSum_BankSchm( pd_dateStart In Date -- ������ ���������� ���������
                                       , pd_dateEnd In Date -- ���������� ���������
                                       , pn_agrId In Number -- ������������� ��������
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- ������ ��� group by - �� ��� no_data_found
      Select Sum(
                Case e.icdetype
                  When 581 Then e.mcdesum
                  When 582 Then -e.mcdesum
                  When 583 Then e.mcdesum
                  When 584 Then -e.mcdesum
                End
                )
      Into nretval
      From cde e
      Where
        e.ncdeagrid = pn_agrId
        And e.dcdedate Between pd_dateStart And pd_dateEnd
        And e.icdetype Between 581 And 584
       -- And e.icdetype+0 in (581, 582, 583, 584) -- ���� ��� �� ����� - ������� ����� � ���� ������
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 ------------------------------------------------------
-- Vct 03.10.2019
-- ��������� ����� ���������� ������������� �� ������
-- � ����� �� �������� ��� ������� �� ����� �����, ����������� �� ������
  Function get_cdeDprPrcSubSum_BankSchm( pd_dateStart In Date -- ������ ���������� ���������
                                       , pd_dateEnd In Date -- ���������� ���������
                                       , pn_agrId In Number -- ������������� ��������
                                       , pn_part In Number  -- ����� �����
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- ������ ��� group by - �� ��� no_data_found
      Select Sum(
                Case e.icdetype
                  When 581 Then e.mcdesum
                  When 582 Then -e.mcdesum
                  When 583 Then e.mcdesum
                  When 584 Then -e.mcdesum
                End
                )
      Into nretval
      From cde e
      Where
        e.ncdeagrid = pn_agrId
        And e.dcdedate Between pd_dateStart And pd_dateEnd
        And e.icdetype Between 581 And 584
        And e.icdepart = pn_part
        -- And e.icdetype+0 in (581, 582, 583, 584) -- ���� ��� �� ����� - ������� ����� � ���� ������
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
-- Vct 03.10.2019
-- ��������� ����� ���������� ������������� �� ������
-- � ����� �� �������� ��� ������� �� ����� ��� (��������������� ���) � ����� �� ��������
  Function get_cdeDprPrcSubSum_MfoSchm( pd_dateStart In Date -- ������ ���������� ���������
                                       , pd_dateEnd In Date -- ���������� ���������
                                       , pn_agrId In Number -- ������������� ��������
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- ������ ��� group by - �� ��� no_data_found
      Select Sum(
                Case e.icdetype
                  When 405 Then e.mcdesum
                  When 406 Then -e.mcdesum
                End
                )
      Into nretval
      From cde e
      Where
        e.ncdeagrid = pn_agrId
        And e.dcdedate Between pd_dateStart And pd_dateEnd
        And e.icdetype Between 405 And 406
       -- And e.icdetype+0 in (405, 406) -- ���� ��� �� ����� - ������� ����� � ���� ������
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
-- Vct 03.10.2019
-- ��������� ����� ���������� ������������� �� ������
-- � ����� �� �������� ��� ������� �� ����� ��� (��������������� ���)
-- ������� ����������� � ������� ������
  Function get_cdeDprPrcSubSum_MfoSchm( pd_dateStart In Date -- ������ ���������� ���������
                                       , pd_dateEnd In Date -- ���������� ���������
                                       , pn_agrId In Number -- ������������� ��������
                                       , pn_part In Number -- ����� �����
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- ������ ��� group by - �� ��� no_data_found
      Select Sum(
                Case e.icdetype
                  When 405 Then e.mcdesum
                  When 406 Then -e.mcdesum
                End
                )
      Into nretval
      From cde e
      Where
        e.ncdeagrid = pn_agrId
        And e.dcdedate Between pd_dateStart And pd_dateEnd
        And e.icdetype Between 405 And 406
        And e.icdepart = pn_part
       -- And e.icdetype+0 in (405, 406) -- ���� ��� �� ����� - ������� ����� � ���� ������
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
 -- NB - ��� ��������� ��� ������ ������ �� ���� mk_current_dpr, �������������� �� ��������.
 --
 -- ��������� ��� �������������  � cdgrp.Recalc_CDD_Item302
 -- ��� ���������� ���������� �������� '1051' - ������� �����������
 -- ������������ ���� ��� ������ ���� ��� - �� ��������� �� 10.04.2018
 -- TODO - ������ ���. ��������, ��������� ��� ������������ � ���������� ���������.
 -- Vct 03.06.2019 - ���� ��� ������� ��� ����� ��������������� ��������,
 -- ����� ��������� ����������� ������� �� ���� ��������������� ������ (153A/152P),
 -- � ������� �� ���� (151A/150P)
 Procedure mk_current_dpr_bnk( pn_agrId in Number -- �������
                             , pn_part in Integer -- ����� - ���� �� ����������...
                             , pd_evt_date in Date -- ����, � ������� ���������� ��������
                             , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                             , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                             --- z.196126 Vct 10.10.2019
                             -- �������� ������������� ��������� ������������� ������� �������� ����� ����������/������������ �������������
                             , pb_correction_required In Boolean --Default False
                             --- Vct 15.05.2020
                             , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- ����� ����������� ���������� ����������
                             , pb_Write_to_grpLog In Boolean -- true - ��������� CDGRP.LOG_PUT(
                            -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                             )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
  i_dpr_pecrent Number; -- ������ �����������
  vd_dpr_enddate Date;  -- ���� ���������� �������� �����������
  -------------------
  -- Vct 02.10.2019 z.195805 - ���������� ��������� �������� ����� ������� � ������ ���
  -- Vct 25/07/2019 - ������� ������� �� ���� �������
  d_modification_start Date; -- ���� ������ �������� ������� ����������� -- Vct 25/07/2019 - ������� ������� �� ���� �������
  b_broken_schedule Boolean; -- ���� ����� ������� � ������� ���.        -- Vct 25/07/2019 - ������� ������� �� ���� �������
                             -- ���� ���������, ���� ����������� ����� ������������ �������������
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- ������� ������������� ���������� ������������ �������������
  b_deprecation_by_part Boolean; -- ���� ����������� �� ������
  m_current_contribution Number:= cn_zero; -- ����� ������� ���������� ������������� �������� � ���� ������� � �������� ������
  --- Vct 17.10.2019
  m_accrued_revenue Number := cn_Zero; -- ����� �������� �������
  ---
  m_clipay Number := cn_Zero;
  d_curctr_Start Date;  --| -- �������� �������, �� ������� ��������� ������� ����� m_current_contribution
  d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- ���� ������� ����������� �  pd_evt_date
  d_prevevt_date Date; -- ���� ���������� ����������� ������������ ����� ����������� ����� ������
  n_dbfixed_acc520 Number:= cn_zero; -- ������� ����� ����������� ����� ������, ��������������� � cdbal
                            -- ����� � ���� pd_evt_date. ��� ������� ������ �� �������� ��������
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; -- �������� � ������� ������� ������� ���������� �������������

  -------------------
  n_dpr_revenue Number:= cn_zero; -- ����� �������� ������ �� �����������
  ----
  m_ovd_ondate Number:= cn_zero; -- ����� ��������� �� ������� ����.
  -- m_client_pay Number; -- ����� ����������� �������. -- ���� ��������� �� �������������
  m_fact_revenue Number:= cn_zero; -- ����� ���������� ���������� �������
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- ����� ����������� ����� ������, �� ������� ����� �������
                             -- 520 ���� �� ���������� ��������
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- ������������� ����� ����� ��� ��������� �������� ������
  vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- ��������� ������� ���������� ���� ���������� ��������� � �����
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
  n_evt_sum Number:= cn_zero;                -- ��������� ���������� ��� �������� ����� �������.
  n_evt_sum2 Number:= cn_zero;               -- ����� ������� � ������ ����� ������������ � ���� ��� ����� � cde
  m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
  --
  m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.

  m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)
  ----
  m_pending_evt_sum Number := cn_zero; -- Vct 26.02.2019 -- ����� �������� ��� ������������ ������������� � ������ ���������� ������� �����������
  ----
  a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
                                        -- Vct 25.02.2019 - �������� �� bp_XNVP_cl
  ------------------------------------------
  nmorningPareSum Number;
  imorningPareSgn pls_integer;
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
  vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
  bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- ��� ������ ����������� ������
  ------------------------------------------
  -- Vct 28.08.2019 - ������� ���������� ������� ���� (pd_evt_date) � �����
  b_flow_modified Boolean;
  i_evtdate_pos Pls_integer; -- ������� � ������, ��������������� ������� ����
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- ���������� ����� ��� ������ ����������� ����� ������
  -----------------------------------        -- ��� ������������� � ������ ����� �������� ��������
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   -- bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   --- Vct 15.05.2020
   bWrite_to_grpLog := pb_Write_to_grpLog;   
   vc_dpr_code := pc_dpr_code; -- ����� ����������� ���������� ����������
   
/*
  -- rem Vct 15.05.2020 - ����������� � mk_current_dpr
   ---------------------------------
   -- ������� ������������� ������� ����������� ( � ������ ��������� ���������)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ������ �������, ���� �� ���������� ������� ������� ����������� �� ��������
      vc_message:='�� �������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����������� �� ������� (�� ��������� ������� ������� ����������� � �������� ��������)';
     -- �������� ��� ��������
     IF bWrite_to_grpLog
       THEN -- ����������� ��� �������� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1841:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr_bnk.1843:'
              , pc_text => vc_message
              );
     END IF;
     Return;
   ELSIF cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
         And (Not cd_dpr_utl.has_deprecation_mark( pn_agrid => pn_agrid
                                                 , pn_part => pn_part)
             )
       THEN -- ���������� ������� ������� ����������� �� ������, �� ��� �������� ������� ����������� �� �����
      vc_message:='������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����� <'||pn_part
                         ||'> �� ���������� ������� ������ ����������� ��� �����.';

     IF bWrite_to_grpLog
       THEN -- ����������� ��� ����� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1859:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr_bnk.1859:'
              , pc_text => vc_message
              );
     END IF;
     Return;

   END IF;

   db_out(pc_prefix => 'cd_dpr.mk_current_dpr_bnk.1869:'
              , pc_text => '  pn_agrid='|| pn_agrid||' pn_part='||pn_part
                           ||' pd_evt_date='||pd_evt_date
                           ||' vc_dpr_code='||vc_dpr_code
              );
  */             
  -----------------------------------------------------------------------            
  -- Vct 02.10.2019
  -- ���� ��������� ����,  ������ �������� ����������� �� �������� ����������� � ���������������...
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- ��������� ������ � ���� ���������� �����������
  Begin

    -- �������� ������ �����������.
    /* rem Vct 14.02.2019
    -- TODO - ����� ���� �� ������������ ����������� ������ ������ ����������� �� �������!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- �������
                                        , pd_evt_date -- r.DCDASIGNDATE -- ����, �� ������� �������� ������ �����������, ���� �� ���� ����������...
                                         )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- �������� ������ �����������.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- ����� �����
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������/�����, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- �������
                                                   , pn_part => pn_part -- �����
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ������ �������:
    -- ���� ������ �� ������������ (��� ����������) ��� ����� ���� (����������� ����������)
    -- ��� ���� ���������� ����������� ������������ pd_evt_date,
    -- �� ��������� ������ ���������
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : ����������� ��������� ��� �� ����������';

        IF bWrite_to_grpLog
          THEN
          CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1917:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
        ELSE
          db_out('cd_dpr.mk_current_dpr_bnk.1919:'
                 , vc_message
                );
        END IF;
        RETURN;
    END IF;
    vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' b_deprecation_by_part='||case b_deprecation_by_part When true then 'True' Else 'False' end
                     ;
    db_out('cd_dpr.mk_current_dpr_bnk.1929:'
          , vc_message
          );
    -------------------------------------------------
  End;
  ---------------------------------------------------------
  -- 2) �������� ������� �� ����� 520 � ���� ��� �������������
  Begin
    -------------------------------------------------
    -- �������� ������� ����� 520 -------------------
    -- �������� ������� � ��� ���� �� ����� ����������� ����� ������
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c ������ ����������� �� ������
                              , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- ������� ��� ����������� ��������� �����������
           -- ���� ���-�� ������ ��������� �� ���������.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- �������� ������� ����� 520 �� ���������� ����  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c ������ ����������� �� ������
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- ���� �������������� �����������
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- � pd_evt_date ����������� �� �����������.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '����. �������-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' ��������� ������='|| case b_dpr_was_today When True Then '��' Else '���' end
               ||' n_dbfixed_acc520:='||cd_utl2s.num_to_str_dot(n_dbfixed_acc520)
               ||' vc_dpr_code='||vc_dpr_code
               ||' pn_part='||pn_part;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.1986:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1991:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    ---------------------------------------------------------
  End;
  -----------------------------------------------------------
  -- 2a ��������� ������� ���� 153A/152P
  Begin

      -- Vct 17.10.2019
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- �������
                                                                          , pn_part => pn_part -- �����
                                                                          , pd_ondate => pd_evt_date - 1 -- ����
                                                                           );
      ELSE
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- �������
                                                                        , pd_ondate => pd_evt_date - 1 -- ����
                                                                         );
      END IF;
    ----------
    /* rem Vct 17.10.2019
    IF NOT b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
      THEN -- ����������� �� ��������
      nmorningPareSum := cdbalance.get_CurSaldo2m( agrID_in   => pn_agrid  -- �������
                                                 , TYPEACC1_in => 153  -- 1� ������������(+) ��� �����
                                                 , TYPEACC2_in => 152  -- 2� ����������(-) ��� �����
                                                 , PART_in   => Cast(NULL as Number) -- �����
                                                 , SUBTYP_in  => Cast(NULL as Number) -- ������ �����
                                                 , defdate_in  => (pd_evt_date - 1) -- ����, �� �������
                                                 );
    ELSE
      -- ����������� �� �����
      nmorningPareSum := cdbalance.get_CurSaldo2m( agrID_in   => pn_agrid  -- �������
                                                 , TYPEACC1_in => 153  -- 1� ������������(+) ��� �����
                                                 , TYPEACC2_in => 152  -- 2� ����������(-) ��� �����
                                                 , PART_in   => pn_part -- �����
                                                 , SUBTYP_in  => Cast(NULL as Number) -- ������ �����
                                                 , defdate_in  => (pd_evt_date - 1) -- ����, �� �������
                                                 );
    END IF;
    */
    --
    imorningPareSgn := Sign(nmorningPareSum);
    -- imorningPareSgn
    -- nmorningPareSum
  End;
  -----------------------------------------------------------
  -- Vct 02.10.2019 z.195805 - ���������� ��������� �������� ����� ������� � ������ ���
  -- 25.07.2019 - �������� �������. (��� �� �������!!!)
  ---------------------
  -- 3) ����������, ��� �� ���� ������� ��������/��������� � ������� ���
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- ������������� ��������
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- ����� �����,
                                  , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- ������� ����, �� ������� ���������� ����� �����������
                                  , pb_isdbms => pb_isdbms                       -- True - ����������� dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- ���� ������ �������� ��
                                  );
    ----
    -- ��������� TODO !!! - ����� ������������ ��������� ������� �������,
    -- ������ ���� �� ��������� � ����, �������� �� ����.
    -- ��� ��������� ��������� � ���������, ����� ������� ���������
    -- �� ��������� ����� vd_dpr_enddate � pd_evt_date ���� ��� �� �����, � ������ �������,��� ��� � �� �������� �����.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���� ������ ������� d_modification_start:='||fmt_date_out(d_modification_start)
               ||' b_broken_schedule:='|| case b_broken_schedule When True Then 'True' Else 'False' end;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2022:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
      CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2022:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;

  ---------------------------------------------------------
  -- 4) ���������� ����� �������� ���������������� ������ �� ������
  Begin
    /* -- rem Vct 22.11.2018
    -- ��������� �������� ������ �� �����������
    n_dpr_revenue := calc_DPR_Revenue( pf_rate_in => i_dpr_pecrent -- ��������� %%� ������ (����� - ������ �����������)
                                      , pd_startdate => d_prevevt_date -- ���� ������ ���������
                                      , pd_enddate => pd_evt_date   -- ���� ���������� ���������
                                      , ppf_pmt => n_dpr_full_money_prev    -- �����, �� ��������� � ������� ����������� �����
                                      );
    */
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ������ �������������� � ������ ���� ������ ������������ ��� �������� � �����.
    -- ������� �� �������� ����� � ������ ������� �����������.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- �������, ��� �������� ���������� ����������� ������
                               , pd_evtDate => pd_evt_date  -- "���� �������" ��� "������� ����", ��������, ����� ������ ����� ��������� �  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- ��������� %%� ������ (����� - ������ �����������)
                               , pd_startdate => d_prevevt_date -- ���� ������ ���������
                               , pd_enddate => pd_evt_date   -- ���� ���������� ���������
                               , ppf_pmt => n_dpr_full_money_prev    -- �����, �� ��������� � ������� ����������� �����
                                                      -- �� �������� �������������, ���� �������� ���������� �������
                               , pb_isdbms => true --pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ������� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' � d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' �� pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' � ����� n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' �� ������ i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2066:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.2066:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    -- TODO - ����� �������� ���������� !!! ����� ������ ����� ����������� ����� ����
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                              , pi_part => pn_part -- �����
                              , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- ����, �� ������� ������ ���� 400� ��������
    ----- ��������� � �������� ������� ������ ��� �������� �� 400�� ��������, 521 ����
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => 400   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ������ ����� 521� (��������������� �����))
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => '������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���������� � ���� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
    END IF;

    IF pb_isdbms AND Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2106:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2106:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  ---------------------------------------------------------
    ---------------------------------------------------------
  -- 5) �������� �������� � ������������ ���� ���������� �������������
  ---------------------------------------------------------
  Begin
  ---------------------------------------------------------
  /* -- ���� ���������� �� �������������
  -- �������� ����� ���������� �������� �� ������ d_prevevt_date - pd_evt_date
  Begin
    -- ��� ����� � �������������� ������ ����������� ��� ���������� ����������� ����� ������
    -- ���� ���������� �� �������������
    m_client_pay := CDSTATE2.Get_ClientPay_LPCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
  End;
  */
  ---------------------------------------------------------
    -- �������� ����� ����������� (!!!) --���������� ���������� ������� �� ������
    Begin
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- ��� ��� ������ �� ������.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- ����� �����
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- � ����� �� ��������
         m_fact_revenue := CDSTATE2.Get_Profit_PCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
      END IF;
    End;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' m_fact_revenue='||m_fact_revenue
                    ||' nmorningPareSum='||nmorningPareSum
                    ;
    END IF;

    IF pb_isdbms AND Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2267:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2267:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    ----------------------------------------------------------
    -- 6) ��������� ����� �������� �� ���������� ���������� �������������
    -- Vct 02.09.2019 - ��� �������� ������ ���� 152/153 �� ����� ���������� �������������
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
      m_revenue_diff := n_evt_sum; -- ����� ���������� ������������� � ������ �����.
      --------------------------
      /*
      i_evt_code := Case
                      When n_evt_sum > 0 Then 405 -- D151(%%������������� - �) - C154 (����� �)
                      When n_evt_sum < 0 Then 406 -- D155(������ A) - C150 (%% ������������� �)
                      Else 0
                    End;
      */
      --------------------------
      -- Vct 05.06.2019
      -- imorningPareSgn
      -- nmorningPareSum
      i_evt_code := Case
                      When  imorningPareSgn = 1 -- �������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                       Then
                         -- D 153 - C 154
                         581
                      When imorningPareSgn = 1 -- �������� ������� �� ����
                         And n_evt_sum < 0.0 --��������� �������������
                       Then
                         -- D155 - C 153
                         582
                      When imorningPareSgn = -1 -- ��������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                         Then
                         -- D152 - C154
                         583
                      When imorningPareSgn = -1 -- ��������� ������� �� ����
                        And n_evt_sum < 0.0 -- ��������� �������������
                         Then
                          -- D155 - C152
                          584
                      When imorningPareSgn = 0 -- �������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                         Then
                           -- D 153 - C 154
                           581
                      When imorningPareSgn = 0 -- �������� ������� �� ����
                        And n_evt_sum < 0.0 -- ��������� �������������
                         Then
                          -- D155 - C152
                          584
                      Else
                        0
                      End;
      ----------------------------
      n_evt_sum := Abs(n_evt_sum);
      -- 05.06.2019
      n_evt_sum2 := n_evt_sum;  -- ��������� ���������� ������������������ ���� ����� ����, ���� ������ ����������� ���������� ��������.
      ----------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (��������������� �����) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (����������� �����) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
                    ||' (n_evt_sum):='||cd_utl2s.num_to_str_dot(n_evt_sum)
                    ||' i_evt_code :='||i_evt_code;
      END IF;
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.1930:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        -----
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1938:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -----------------------------------------------------
      -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
      -- ����� ������������ ����� ��������� ������������� ��������
      /* -- rem Vct 05.06.2019 TODO - ���� ����� ���������� ������������.
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
        -- TODO - ������ �����, ����� ���. ��������...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => 405
                                         , pi_evt_anti => 406
                                          );
        IF n_evt_sum2 != cn_Zero
          THEN
          i_evt_code := Case
                          When n_evt_sum > cn_Zero Then  405
                          When n_evt_sum < cn_Zero Then  406
                          Else ci_Zero
                        End;
        END IF;
      END IF;
      */
      ----------------------------------------------------
      IF i_evt_code != ci_Zero
        And n_evt_sum2 != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                   , pa_evt_queue => pa_result
                                   , p_ncdeAgrid => pn_agrID      -- �������
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                   , p_icdeType => i_evt_code   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ���� 521)
                                   , p_dcdeDate => pd_evt_date -- ���� �������
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => '������������� �����. ������� �������� '||cd_utl2s.num_to_str_dot(pn_agrId)  -- ����������� � ��������  -- ����������� � ��������
                                   -- Vct 15.10.2018 z. -- ��� 405/406 �������� �� ������ ���� �������������
                                   , pc_Declarative => to_char(Null)
                                   );
      END IF;
      -----------------------------------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message :=  ' n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                     ||' i_evt_code :='||i_evt_code;
      END IF;
      --
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.2231:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2231:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
    End;
    -----
  End;
  ---------------------------------------------------------
  -- ��������� ����� ��� ������ �� ���������� ������� �� 520 �����
  -- ��������� ����� ������������ ������������� ��� ����� �������
  ---------------------------------------------------------
  ---------------------------------------------------------
  ---- ���������� ����� ��������� � ������� ���
  Begin
    ------- ������ ���������, ������������ �� ������� ���� (���� ������� ��������� �������� ���) ----
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- ������������� ��������
                                                    , pn_part => pn_part -- �����
                                                    , pc_code => vc_dpr_code -- ��� ������� ������� ����������� - �� �����, �������� ��� �� �������
                                                    , pd_onDate => pd_evt_date -- ����, �� ������� ������������ ������� (�� �����)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2269:'
               , '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2269:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- �������� ������������� ����� ����� ��� ��������� �������� ������,
  -- ���� ������, ����� �������������� "����������� �����"
  -- + �������� ������ ���������� ����, � ������� ����� ��������� ����� ���������
  Begin
      --------------------------------------------
      -- �������� �����, ��������������� ���������� ���������
      --------------------------------------------
      -----
      -- �������� ������������� ����� �����, ������������ ���������������� ������ ����������� ������
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- �������� ��������� ������� ����� ��������� � ������.
      -- ���� ����� ������� �� ����� �� ���� ������ �� ��������.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- ���������� ����� ������� ����������� ����� ������,
  -- � ����� ����������� ����� ������ �� ����������� �������
  -- ����� ����� ��������� �������� �� ����� ����������� ����� ������
  -- � ����� ������� ������������ �������������
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- ����� ����������� ����� ������, �� ������� ����� �������
      --                       -- 520 ���� �� ���������� ��������
      --  m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
      -----------------------------------------------------------------------------------
      bp_XNPV_lc( pn_agrId => pn_agrId -- �������
                -- Vct 14.02.2019
                , pn_part => pn_part -- ����� �����,
                , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                --
                , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                , pc_odvclc_code  => vc_odvclc_code  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                , pd_flow_date => pd_evt_date -- ����, �� ������� ������� �������� �����
                , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                , pm_ovd_sum => m_ovd_ondate -- ����� ���������, ������� ���������� ������ ��� �������
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- ������� ����� ��� ������ ����������� ����� ������
                -- Vct 28.08.2019 - ������� ��������� ������� ���� (pd_evt_date) � �����
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
                );
      ---------------------------
      -- ���� ��� ���� �������, ���������� ����� ����� ������ �� ������� �������
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

     -- Vct 25/07/2019 - ������� ������� �� ���� �������
     -- IF b_broken_schedule
     --   THEN -- ��� ���� �������
           -- Vct 25.02.2019 ���������� ����������� ����� ������� ���������� �������������
           -- �� �������� (������) ������� ���������� ����� ������� ���������� �������������
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- ������� �����
                                     , pn_rate => i_dpr_pecrent  -- ������ (�����������), ����������� �����
                                     , pd_reper_date => pd_evt_date -- ������� ����, �� ��������� � ������� ��������� ����� ������� ���������� �������������
                                     -- Vct 05.03.2019 , Vct case 28/08/2019 z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                     -- Vct 11.10.2019 z.196254 - ������ ������
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- ���������� ����� ������� ���������� ������������� � ������ �����.
                                     );

       /* -- rem Vct 26.02.2019
           -- ��������� ����� ����� ������ �� ������� �������
        bp_XNPV_lc( pn_agrId => pn_agrId -- �������
                  -- Vct 14.02.2019
                  , pn_part => pn_part -- ����� �����,
                  , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                  --
                  , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                  , pc_odvclc_code  => vc_odvclc_code  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                  , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                  , pd_flow_date => (d_modification_start - 1) -- ����, �� ������� ������� �������� �����
                  , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                  , pm_ovd_sum => m_ovd_ondate -- ����� ���������, ������� ���������� ������ ��� �������
                  , pm_sum_after_mn => m_dpr_after_prev
                  );
                  -- ���������� ����������� ����� ������� ���������� �������������
      ELSE
        -- ����� ������� �� ����.
        -- ����� �� ����������� ������� ������� ������ ������� ����� ����� ������
        m_dpr_after_prev := m_dpr_after_curent;
       */
  --    END IF; -- Vct 25/07/2019 - ������� ������� �� ���� �������
  End;
  ----------------------------------------------------------------
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ���� ������������ ������������� (403/404)
  -- TODO !!! - �������� ������������ ��������� ����.
  Begin

  -- Vct z.195805 - ����������� ������������ ������������� ������:
  -- �) ��� ��������� ����������� -- TODO - ���� �����������, ��� ��������� ����������� ������� ����������� ���������...
  -- �) ��� ������������ ������� �������
  -- b) ��� �������� ������� �� � ���� ��������� �������
    --
    -- Vct 09.10.2019
    IF b_flow_modified
       And (Not Coalesce(pb_correction_required, False)) --����  ����� ����������� ������� ���� �� ������������ ����� ��� ��� �������� ������� � ��������� ������� ������.
                                                         -- ������� �� ����� �� ���������, ���� ���� �������� ���������� ���������
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- ����� �����
                                      , p_DFrom => (d_prevevt_date + ci_One)
                                      , p_DTO  => pd_evt_date
                                        );
        Else
          m_clipay := Get_CliPay(p_AGRID =>  pn_agrId
                               , p_DFrom  => (d_prevevt_date + ci_One)
                               , p_DTO   => pd_evt_date
                                );
        END IF;
    END IF;

---------------------------------

    -- Vct 02.10.2019 z.195805 -- ������� ������������� ���������� ������������ �������������
    b_need_pareCorrection := pb_correction_required -- �������� ������������� �������� ������� ����� ������������ �������������
                       Or b_broken_schedule -- ������� ������ ������
                       OR (b_flow_modified
                          And m_clipay != cn_zero -- m_fact_revenue != cn_Zero -- z.19805 - ������ ������������ ������������� �������� ������ ��� ������� �������� ������ �� ����������� ����
                          -- z.196758 � �������������� ���� �� �������� ������� �� ���� ��������� 520�� �����...
                          -- (����� ���� �������� � ����������������� ��� �������...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                          --    And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate)
                              )
                          )
                          ;  -- ���������� ������ �� ���� ��������� �������, ���� �������� �� �������� ��������� ���������� ������� ���� � ������


   IF b_need_pareCorrection
     THEN

     -- Vct 17.10.2019
     IF b_flow_modified
       THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );
          -----
          /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- �������
                                               , pn_part => pn_part
                                              , pc_RT => 'T' -- ��� ������� %%, ��� �������� �������� �����
                                                             -- ���� �������, ��� ������ ������ �� T? ������ ���������
                                              , pd_onDate => pd_evt_date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                                              );
         */
        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );
         /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- �������
                                                , pc_RT => 'T' -- ��� ������� %%, ��� �������� �������� �����
                                                               -- ���� �������, ��� ������ ������ �� T? ������ ���������

                                                , pd_onDate => pd_evt_date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                                                );
        */
        END IF;

     END IF;

     /* rem Vct 11.10.2019 z.196254 - ������������ ���� �� ����� ����������...
     -- Vct z.195805 02.10.2019
     -- ��������� ������ ���������� ������������� � ��������� ����� ���������� ��������� ������
     IF b_flow_modified
       THEN
         -- ���������� �������� ����� ������� ���������� �������������
       --  d_curctr_Start Date;  --| -- �������� �������, �� ������� ��������� ������� ����� m_current_contribution
        IF i_evtdate_pos > 1 -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
          THEN

           d_curctr_Start :=   a_potok_xnpv(i_evtdate_pos - 1).ddate;
           d_curctr_Start := d_curctr_Start + 1; -- ��������� ���� �� "�����������" �������� � ������
        Else
           d_curctr_Start := cd_chdutils.cd_zero_cd_date; -- �� ������ �����
        END IF;
       ----
       d_curctr_End := pd_evt_date - 1;    -- ���������� ���� �� ��������� �� ���� ���������� ���
       ----
       IF  b_deprecation_by_part
         THEN
         -- ������� �� �����
         m_current_contribution := get_cdeDprPrcSubSum_BankSchm( pd_dateStart => d_curctr_Start -- ������ ���������� ���������
                                                               , pd_dateEnd => d_curctr_End -- ���������� ���������
                                                               , pn_agrId => pn_agrid  -- ������������� ��������
                                                               , pn_part => pn_part -- ����� �����
                                                               );

       ELSE
         -- �� �������� � �����
         m_current_contribution := get_cdeDprPrcSubSum_BankSchm( pd_dateStart => d_curctr_Start -- ������ ���������� ���������
                                                               , pd_dateEnd => d_curctr_End  -- ���������� ���������
                                                               , pn_agrId => pn_agrid  -- ������������� ��������
                                                               );
       END IF;

     END IF;
     */
     ----------------------------------------------
-- Vct 25/07/2019 - ������� ������� �� ���� �������
-- Vct 25.02.2019 -- ����� ������������ ������������� ������� ������ ���� ������� ��� ���� �������
--    IF b_broken_schedule   -- Vct 26.02.2019
--      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      ------------------------
      /* rem Vct 02.10.2019
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' ���� ������� � ���������� ������ ������ m_dpr_after_curent:='||cd_utl2s.num_to_str_dot(m_dpr_after_curent)
                    --||' m_dpr_after_prev:='||cd_utl2s.num_to_str_dot(m_dpr_after_prev)
                    --||' delta:='|| cd_utl2s.num_to_str_dot(m_dpr_after_curent - m_dpr_after_prev)
                    ||' m_pending_diff='||cd_utl2s.num_to_str_dot(m_pending_diff)
                    ;
      END IF;

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.2385'
                 , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2385:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      */
      ------------------------
      -- ������������ ������������� ��� ����� �������.
      -- �������� ��� ����� ���������������, ��� � �������������� ������ ������ Vct 25.12.2018
      -------------------------------------------------------------------
      -- ��� ������, ��������� ���� m_dpr_after_curent - m_dpr_after_prev
      -- n_evt_sum := m_dpr_after_curent - m_dpr_after_prev; -- rem Vct 26.02.2019

      ----------------------------------------------------------
      -- Vct 25.02.2019 ������ ������ ������� ������������ �������������
      -- ������ ����� ������� ��� ������� ����� ������� ���������� �������� ���������� �� ���� ��� �����
      -- ����� ����������� ������� ������������ ������������� � ������ ������� ���������� �������������

      -- m_pending_diff -- ����� ������� ���������� �������������
      -- ���� m_pending_diff ������������, �� �� ��������� ���������� �����, ���� ������������ �� ������

      -- m_revenue_diff Number; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
      -- m_pending_diff Number; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)

      -- ����� �������� ����� � cdbal ((153-152)-(150-151))
      -- �� �������������� ����
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- �������
                                                                          , pn_part => pn_part -- �����
                                                                          , pd_ondate => pd_evt_date - 1 -- ����
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- �������
                                                                        , pd_ondate => pd_evt_date - 1 -- ����
                                                                         );
      END IF;

      -- �������... TODO.. �������� ������� ������...
      vc_message := ' m_pending_current_rest before  ='||m_pending_current_rest
                  ||' m_revenue_diff='|| m_revenue_diff -- -- ����� ���������� ������������� � ������ �����.
                  ||' (m_pending_current_rest + m_revenue_diff)='||(m_pending_current_rest + m_revenue_diff)
                  ||' m_pending_diff='||m_pending_diff  -- ������ �������� ������� ���� ������������ �������������
                  ||' m_current_contribution = '||m_current_contribution
                  ||' d_curctr_Start='||to_char(d_curctr_Start,'DD.MM.YYYY')
                  ||' d_curctr_End='||to_char(d_curctr_End,'DD.MM.YYYY')
                  ||' m_accrued_revenue='||m_accrued_revenue
                  ;
      db_out('cd_dpr.mk_current_dpr_bnk.2431'
             , vc_message
            );

      -- ��������� ����� ����������� ���������� �������������
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- � ����� ������� ������������� ��������� ������ �������� �������
      m_pending_diff := m_pending_diff + m_accrued_revenue;

       /* rem Vct 11.10.2019 z.196254
      -- 05.09.2019 Vct - �������� ���������� ������������� �������� ��� �� ������ ������� �������������
      -- ��� ������ ����� � ������� ����� ���� ������������� ��������� ������������ ������
      -- (�������������� ��� ���������� ��������� ������ � ������� ���.)
      IF b_flow_modified
        THEN
        -- 02.10.2019 z.195805 ����� ��� ������ ����� ���������� �������������
        --m_pending_current_rest := m_pending_current_rest + m_current_contribution;
        --------------------------------------------------------------------------
        -- �� ���������� �������� ����...
         m_pending_diff := m_pending_diff - m_revenue_diff - m_current_contribution;
         vc_message := ' m_pending_diff='||m_pending_diff;
        db_out('cd_dpr.mk_current_dpr_bnk.2471'
             , vc_message
            );
      END IF;
      */
      m_pending_evt_sum := cn_Zero;

      -- ��������� ����� ������������ �������������
      IF    m_pending_current_rest > cn_Zero -- ����� ��� �����
        And m_pending_diff <= cn_Zero        -- ����� ���� ����� �����
        THEN -- ���������� ������������� ����������� ����� � ���������� ����� ���� ��������
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ����� ��� ������
        And m_pending_diff >= cn_Zero         -- ������� ���� ������
        THEN -- ���������� ������������� ����������� ������ � ���������� ����� ���� ���������
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ����� ��� �����
        And m_pending_diff > cn_Zero         -- ������� ������
        THEN -- �������� ���� � ������ �� ������
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ����� ��� ������
         And m_pending_diff < cn_Zero        -- ����� ����� �����
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;
     ----- z.194702 Vct 26.07.2019
      i_evt_code := Case
                    When imorningPareSgn = 1 -- �������� ������� �� ����
                      Then
                      Case
                      When n_evt_sum > cn_Zero Then 401 --404 --401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 403 --402 -- 404 (���)
                      Else ci_Zero
                      End
                    When imorningPareSgn = -1 -- ��������� ������� �� ����
                      Then
                      -- TODO - ��� ��� ���������� ����������� �������� - ���� ���������.
                      Case
                      When n_evt_sum > cn_Zero Then 404 --401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (���)
                      Else ci_Zero
                      End
                    Else  -- ������� ������� �� ����
                      Case
                      When n_evt_sum > cn_Zero Then  401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (���)
                      Else ci_Zero
                      End
                    End;

      n_evt_sum := Abs(n_evt_sum);
      IF i_evt_code != ci_Zero
        And n_evt_sum != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_evt_sum
                                  , p_ccdeRem => '������� ������������ ������������� ��. �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 -- Vct 15.10.2018 z. -- ��� 403/404 �������� �� ������ ���� �������������
                                 , pc_Declarative => to_char(Null)
                                 );
        END IF;


--      END IF; -- Vct 25/07/2019 - ������� ������� �� ���� �������
    END IF; -- Vct 02.10.2019
  End;
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ������� �� 520 ����� (������� 409 ��� ������������� ����, 410 - ��� �������������)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
    --  n_evt_sum Number;                -- ��������� ���������� ��� �������� ����� �������.

-- Vct 25/07/2019 - ������� ������� �� ���� �������
--    IF b_broken_schedule   -- Vct 26.02.2019
--      -- Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN -- !!!! ����� ������� ���-�� �������!
      -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + (m_dpr_after_curent - m_dpr_after_prev); -- ������ ����� ��� ����� ������������ �������������
      -- Vct 26/02/2016
     
      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum;
      
--      -- Vct 05.10.2019
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    ELSE
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    END IF;

    -- Vct 22.06.2018
    -- 520 �������� ��������.
    -- ���������� ���������������
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (������) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code)
                  ||' n_dpr_full_money_prev='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
                  ||' m_dpr_after_curent='||cd_utl2s.num_to_str_dot( m_dpr_after_curent)
                  ||' (n_dpr_full_money_prev - m_dpr_after_curent)='||(n_dpr_full_money_prev - m_dpr_after_curent);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2524:'
               ,vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2524:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;

    -----------------------------
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
      -- TODO - ������ �����, ����� ���. ��������...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => 409
                                       , pi_evt_anti => 410
                                        );
      IF n_evt_sum2 != cn_Zero
        THEN
        i_evt_code := Case
                        When n_evt_sum > cn_Zero Then  410
                        When n_evt_sum < cn_Zero Then  409
                        Else ci_Zero
                      End;
      END IF;
    END IF;
    ----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (� ����:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2574:'
               , vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2574:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;
    -----------------------------
    ----- ��������� � �������� ������� ������ ��� �������� �� 409/410�� ��������, 520 ����
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => '������������� �������� ������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - ��������� �������� ����������� ������ z.190837
  -- � ����� ���������� ���������� � ��������/�����/����
  -- ����, ��� ��� �� ������ ����������...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - ��� ����� ���� �� ������������ ��� ���������� ����������� ������ ������ � �����-�� "��������� �������"
                                 -- ���� �� ����������...
      vc_message_text := SUBSTR(cd_errsupport.format_ora_errorstack(True),1,2000);
      vc_message_text := substr('cd_dpr.mk_current_dpr_bnk:ERROR:(pn_agrId='||pn_agrId
                             ||' pn_part='||pn_part
                             ||' pd_evt_date='||to_char(pd_evt_date,'DD.MM.YYYY')
                             ||'):'
                             ||vc_message_text, 1,2000);

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.2621:'
                , vc_message
                );
      --    raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
      ELSIF bWrite_to_grpLog
        THEN
          -- TODO: ��������, ��� ����������� ������, ��������� ������� ��� ������ �� ������������ ������.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- �������� ��� ������...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;
 -------------------------------------------------------------------
  Procedure mk_current_dpr_bnk_fv( pn_agrId in Number -- �������
                             , pn_part in Integer -- ����� - ���� �� ����������...
                             , pd_evt_date in Date -- ����, � ������� ���������� ��������
                             , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                             , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                             --- z.196126 Vct 10.10.2019
                             -- �������� ������������� ��������� ������������� ������� �������� ����� ����������/������������ �������������
                             , pb_correction_required In Boolean --Default False
                             --- Vct 15.05.2020
                             , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- ����� ����������� ���������� ����������
                             , pb_Write_to_grpLog In Boolean -- true - ��������� CDGRP.LOG_PUT(
                            -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                             )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
  i_dpr_pecrent Number; -- ������ �����������
  vd_dpr_enddate Date;  -- ���� ���������� �������� �����������
  -------------------
  -- Vct 02.10.2019 z.195805 - ���������� ��������� �������� ����� ������� � ������ ���
  -- Vct 25/07/2019 - ������� ������� �� ���� �������
  d_modification_start Date; -- ���� ������ �������� ������� ����������� -- Vct 25/07/2019 - ������� ������� �� ���� �������
  b_broken_schedule Boolean; -- ���� ����� ������� � ������� ���.        -- Vct 25/07/2019 - ������� ������� �� ���� �������
                             -- ���� ���������, ���� ����������� ����� ������������ �������������
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- ������� ������������� ���������� ������������ �������������
  b_deprecation_by_part Boolean; -- ���� ����������� �� ������
  
  --m_current_contribution Number:= cn_zero; -- ����� ������� ���������� ������������� �������� � ���� ������� � �������� ������
  
  --- Vct 17.10.2019
  --m_accrued_revenue Number := cn_Zero; -- ����� �������� �������
  ---
  --m_clipay Number := cn_Zero;
  --d_curctr_Start Date;  --| -- �������� �������, �� ������� ��������� ������� ����� m_current_contribution
  --d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- ���� ������� ����������� �  pd_evt_date
  d_prevevt_date Date; -- ���� ���������� ����������� ������������ ����� ����������� ����� ������
  n_dbfixed_acc520 Number:= cn_zero; -- ������� ����� ����������� ����� ������, ��������������� � cdbal
                            -- ����� � ���� pd_evt_date. ��� ������� ������ �� �������� ��������
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; -- �������� � ������� ������� ������� ���������� �������������
  -- Vct 07.04.2021 
  m_520plan Number := cn_Zero; -- �������� ������� 520�� �����, �� ������� ����� ������� �������� ����� ������ (520) ����, ������ �� ������.
  m4correction Number := cn_Zero; -- ����� ����� ���� �������������, �������� 520� ���� � ����� �����, ��� �������� � m_520plan.
  -------------------
  n_dpr_revenue Number:= cn_zero; -- ����� �������� ������ �� �����������
  ----
  --m_ovd_ondate Number:= cn_zero; -- ����� ��������� �� ������� ����.

  m_fact_revenue Number:= cn_zero; -- ����� ���������� ���������� �������
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- ����� ����������� ����� ������, �� ������� ����� �������
                             -- 520 ���� �� ���������� ��������
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
  -----------------------------------------
  --vc_extflowsqlid cd_mda.cmda_ac828%Type; -- ������������� ����� ����� ��� ��������� �������� ������
  --vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- ��������� ������� ���������� ���� ���������� ��������� � �����
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
  n_evt_sum Number:= cn_zero;                -- ��������� ���������� ��� �������� ����� �������.
  n_evt_sum2 Number:= cn_zero;               -- ����� ������� � ������ ����� ������������ � ���� ��� ����� � cde
  --m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
  --
  --m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.

  --m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)
  ----
  --m_pending_evt_sum Number := cn_zero; -- Vct 26.02.2019 -- ����� �������� ��� ������������ ������������� � ������ ���������� ������� �����������
  ----
  --a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
                                        -- Vct 25.02.2019 - �������� �� bp_XNVP_cl
  ------------------------------------------
  nmorningPareSum Number;
  imorningPareSgn pls_integer;
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
  vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
  bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- ��� ������ ����������� ������
  ------------------------------------------
  -- Vct 28.08.2019 - ������� ���������� ������� ���� (pd_evt_date) � �����
  --b_flow_modified Boolean;
  --i_evtdate_pos Pls_integer; -- ������� � ������, ��������������� ������� ����
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- ���������� ����� ��� ������ ����������� ����� ������
  -----------------------------------        -- ��� ������������� � ������ ����� �������� ��������
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   -- bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   --- Vct 15.05.2020
   bWrite_to_grpLog := pb_Write_to_grpLog;   
   vc_dpr_code := pc_dpr_code; -- ����� ����������� ���������� ����������
   
  -----------------------------------------------------------------------            
  -- Vct 02.10.2019
  -- ���� ��������� ����,  ������ �������� ����������� �� �������� ����������� � ���������������...
  -- (�� mk_current_dpr)
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- ��������� ������ � ���� ���������� �����������
  Begin

    -- �������� ������ �����������.
    /* rem Vct 14.02.2019
    -- TODO - ����� ���� �� ������������ ����������� ������ ������ ����������� �� �������!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- �������
                                        , pd_evt_date -- r.DCDASIGNDATE -- ����, �� ������� �������� ������ �����������, ���� �� ���� ����������...
                                         )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- �������� ������ �����������.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- ����� �����
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������/�����, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- �������
                                                   , pn_part => pn_part -- �����
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ������ �������:
    -- ���� ������ �� ������������ (��� ����������) ��� ����� ���� (����������� ����������)
    -- ��� ���� ���������� ����������� ������������ pd_evt_date,
    -- �� ��������� ������ ���������
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : ����������� ��������� ��� �� ����������';

        IF bWrite_to_grpLog
          THEN
          CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk_fv.3287:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
        ELSE
          db_out('cd_dpr.mk_current_dpr_bnk_fv.3289:'
                 , vc_message
                );
        END IF;
        RETURN;
    END IF;
    ---
    vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' b_deprecation_by_part='||case b_deprecation_by_part When true then 'True' Else 'False' end
                     ;
    db_out('cd_dpr.mk_current_dpr_bnk_fv.3301:'
          , vc_message
          );
    -------------------------------------------------
  End;
  ---------------------------------------------------------
  -- 2) �������� ������� �� ����� 520 � ���� ��� �������������
  Begin
    -------------------------------------------------
    -- �������� ������� ����� 520 -------------------
    -- �������� ������� � ��� ���� �� ����� ����������� ����� ������
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c ������ ����������� �� ������
                              , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- ������� ��� ����������� ��������� �����������
           -- ���� ���-�� ������ ��������� �� ���������.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- �������� ������� ����� 520 �� ���������� ����  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c ������ ����������� �� ������
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- ���� �������������� �����������
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- � pd_evt_date ����������� �� �����������.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '����. �������-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' ��������� ������='|| case b_dpr_was_today When True Then '��' Else '���' end
               ||' n_dbfixed_acc520:='||cd_utl2s.num_to_str_dot(n_dbfixed_acc520)
               ||' vc_dpr_code='||vc_dpr_code
               ||' pn_part='||pn_part;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.1986:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.1991:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    ---------------------------------------------------------
  End;
  -----------------------------------------------------------
  -- 2a ��������� ������� ���� 153A/152P
  Begin

      -- Vct 17.10.2019
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- �������
                                                                          , pn_part => pn_part -- �����
                                                                          , pd_ondate => pd_evt_date - 1 -- ����
                                                                           );
      ELSE
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- �������
                                                                        , pd_ondate => pd_evt_date - 1 -- ����
                                                                         );
      END IF;
      
      imorningPareSgn := Sign(nmorningPareSum);

  End;
  -----------------------------------------------------------
  -- Vct 02.10.2019 z.195805 - ���������� ��������� �������� ����� ������� � ������ ���
  -- 25.07.2019 - �������� �������. (��� �� �������!!!)
  ---------------------
  -- 3) ����������, ��� �� ���� ������� ��������/��������� � ������� ���
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- ������������� ��������
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- ����� �����,
                                  , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- ������� ����, �� ������� ���������� ����� �����������
                                  , pb_isdbms => pb_isdbms                       -- True - ����������� dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- ���� ������ �������� ��
                                  );
    ----
    -- ��������� TODO !!! - ����� ������������ ��������� ������� �������,
    -- ������ ���� �� ��������� � ����, �������� �� ����.
    -- ��� ��������� ��������� � ���������, ����� ������� ���������
    -- �� ��������� ����� vd_dpr_enddate � pd_evt_date ���� ��� �� �����, � ������ �������,��� ��� � �� �������� �����.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���� ������ ������� d_modification_start:='||fmt_date_out(d_modification_start)
               ||' b_broken_schedule:='|| case b_broken_schedule When True Then 'True' Else 'False' end;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2022:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
      CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2022:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;

  ---------------------------------------------------------
  -- 4) ���������� ����� �������� ���������������� ������ �� ������
  Begin
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ������ �������������� � ������ ���� ������ ������������ ��� �������� � �����.
    -- ������� �� �������� ����� � ������ ������� �����������.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- �������, ��� �������� ���������� ����������� ������
                               , pd_evtDate => pd_evt_date  -- "���� �������" ��� "������� ����", ��������, ����� ������ ����� ��������� �  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- ��������� %%� ������ (����� - ������ �����������)
                               , pd_startdate => d_prevevt_date -- ���� ������ ���������
                               , pd_enddate => pd_evt_date   -- ���� ���������� ���������
                               , ppf_pmt => n_dpr_full_money_prev    -- �����, �� ��������� � ������� ����������� �����
                                                      -- �� �������� �������������, ���� �������� ���������� �������
                               , pb_isdbms => true --pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ������� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' � d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' �� pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' � ����� n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' �� ������ i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2066:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.2066:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
      -- �������� ����� ����������� (!!!) --���������� ���������� ������� �� ������
    Begin 
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- ��� ��� ������ �� ������.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- ����� �����
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- � ����� �� ��������
         m_fact_revenue := CDSTATE2.Get_Profit_PCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
      END IF;


      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                      ||' m_fact_revenue='||m_fact_revenue
                      ||' nmorningPareSum='||nmorningPareSum
                      ;
      END IF;

      IF pb_isdbms AND Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk_fv.3489:'
                 , vc_message
                );
      ELSIF  bWrite_to_grpLog
        THEN
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk_fv.3494:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
    End;
        
    -- Vct 07.04.2021 �������� ������� 520, �� ������� ����� ����� � ������ ���� ��������, ����������� � ��������������� � ���.
    m_520plan := n_dpr_full_money_prev + (n_dpr_revenue - m_fact_revenue) ;
    ---------------------------------------------------
    -- ����������� ������������� ������ �� 521 ����� � ������ ����������� ������� ����� ��������... 
    -- TODO - ����� �������� ���������� !!! ����� ������ ����� ����������� ����� ����
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                              , pi_part => pn_part -- �����
                              , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- ����, �� ������� ������ ���� 400� ��������
    ----- ��������� � �������� ������� ������ ��� �������� �� 400�� ��������, 521 ����
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => 400   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ������ ����� 521� (��������������� �����))
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => '������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���������� � ���� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
    END IF;

    IF pb_isdbms AND Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2106:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2106:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  ---------------------------------------------------------
    ---------------------------------------------------------
  -- �������� �������� � ������������ ���� ���������� �������������
  ---------------------------------------------------------
    -- 6) ��������� ����� �������� �� ���������� ���������� �������������
    -- Vct 02.09.2019 - ��� �������� ������ ���� 152/153 �� ����� ���������� �������������
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
     -- m_revenue_diff := n_evt_sum; -- ����� ���������� ������������� � ������ ����� ����� �������������� ��� ���������� ������������ ������������
      --------------------------
      --------------------------
      -- Vct 05.06.2019
      -- imorningPareSgn
      -- nmorningPareSum
      i_evt_code := Case
                      When  imorningPareSgn = 1 -- �������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                       Then
                         -- D 153 - C 154
                         581
                      When imorningPareSgn = 1 -- �������� ������� �� ����
                         And n_evt_sum < 0.0 --��������� �������������
                       Then
                         -- D155 - C 153
                         582
                      When imorningPareSgn = -1 -- ��������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                         Then
                         -- D152 - C154
                         583
                      When imorningPareSgn = -1 -- ��������� ������� �� ����
                        And n_evt_sum < 0.0 -- ��������� �������������
                         Then
                          -- D155 - C152
                          584
                      When imorningPareSgn = 0 -- �������� ������� �� ����
                         And n_evt_sum > 0.0 -- �������� �������������
                         Then
                           -- D 153 - C 154
                           581
                      When imorningPareSgn = 0 -- �������� ������� �� ����
                        And n_evt_sum < 0.0 -- ��������� �������������
                         Then
                          -- D155 - C152
                          584
                      Else
                        0
                      End;
      ----------------------------
      n_evt_sum := Abs(n_evt_sum);
      -- 05.06.2019
      n_evt_sum2 := n_evt_sum;  -- ��������� ���������� ������������������ ���� ����� ����, ���� ������ ����������� ���������� ��������.
      ----------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (��������������� �����) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (����������� �����) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
                    ||' (n_evt_sum):='||cd_utl2s.num_to_str_dot(n_evt_sum)
                    ||' i_evt_code :='||i_evt_code;
      END IF;
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.1930:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        -----
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk_fv.3615:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -----------------------------------------------------
      -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
      -- ����� ������������ ����� ��������� ������������� ��������
      /* -- rem Vct 05.06.2019 TODO - ���� ����� ���������� ������������.
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
        -- TODO - ������ �����, ����� ���. ��������...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => 405
                                         , pi_evt_anti => 406
                                          );
        IF n_evt_sum2 != cn_Zero
          THEN
          i_evt_code := Case
                          When n_evt_sum > cn_Zero Then  405
                          When n_evt_sum < cn_Zero Then  406
                          Else ci_Zero
                        End;
        END IF;
      END IF;
      */
      ----------------------------------------------------
      IF i_evt_code != ci_Zero
        And n_evt_sum2 != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                   , pa_evt_queue => pa_result
                                   , p_ncdeAgrid => pn_agrID      -- �������
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                   , p_icdeType => i_evt_code   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ���� 521)
                                   , p_dcdeDate => pd_evt_date -- ���� �������
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => '������������� �����. ������� �������� '||cd_utl2s.num_to_str_dot(pn_agrId)  -- ����������� � ��������  -- ����������� � ��������
                                   -- Vct 15.10.2018 z. -- ��� 405/406 �������� �� ������ ���� �������������
                                   , pc_Declarative => to_char(Null)
                                   );
      END IF;
      -----------------------------------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message :=  ' n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                     ||' i_evt_code :='||i_evt_code;
      END IF;
      --
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.2231:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2231:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
    End;
  -------
  ---------------------------------------------------------
  -- ��������� ����� ��� ������ �� ���������� ������� �� 520 �����
  -- ��������� ����� ������������ ������������� ��� ����� �������
  ---------------------------------------------------------
  /* rem Vct 08.04.2021 - TODO - �������� � ������������� ��������������� ��������� �� �����...
  ---------------------------------------------------------
  ---- ���������� ����� ��������� � ������� ���
  Begin
    ------- ������ ���������, ������������ �� ������� ���� (���� ������� ��������� �������� ���) ----
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- ������������� ��������
                                                    , pn_part => pn_part -- �����
                                                    , pc_code => vc_dpr_code -- ��� ������� ������� ����������� - �� �����, �������� ��� �� �������
                                                    , pd_onDate => pd_evt_date -- ����, �� ������� ������������ ������� (�� �����)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2269:'
               , '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2269:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- �������� ������������� ����� ����� ��� ��������� �������� ������,
  -- ���� ������, ����� �������������� "����������� �����"
  -- + �������� ������ ���������� ����, � ������� ����� ��������� ����� ���������
  Begin
      --------------------------------------------
      -- �������� �����, ��������������� ���������� ���������
      --------------------------------------------
      -----
      -- �������� ������������� ����� �����, ������������ ���������������� ������ ����������� ������
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- �������� ��������� ������� ����� ��������� � ������.
      -- ���� ����� ������� �� ����� �� ���� ������ �� ��������.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- ���������� ����� ������� ����������� ����� ������,
  -- � ����� ����������� ����� ������ �� ����������� �������
  -- ����� ����� ��������� �������� �� ����� ����������� ����� ������
  -- � ����� ������� ������������ �������������
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- ����� ����������� ����� ������, �� ������� ����� �������
      --                       -- 520 ���� �� ���������� ��������
      --  m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
      -----------------------------------------------------------------------------------
      bp_XNPV_lc( pn_agrId => pn_agrId -- �������
                -- Vct 14.02.2019
                , pn_part => pn_part -- ����� �����,
                , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                --
                , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                , pc_odvclc_code  => vc_odvclc_code  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                , pd_flow_date => pd_evt_date -- ����, �� ������� ������� �������� �����
                , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                , pm_ovd_sum => m_ovd_ondate -- ����� ���������, ������� ���������� ������ ��� �������
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- ������� ����� ��� ������ ����������� ����� ������
                -- Vct 28.08.2019 - ������� ��������� ������� ���� (pd_evt_date) � �����
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
                );
      ---------------------------
      -- ���� ��� ���� �������, ���������� ����� ����� ������ �� ������� �������
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

     -- Vct 25/07/2019 - ������� ������� �� ���� �������
     -- IF b_broken_schedule
     --   THEN -- ��� ���� �������
           -- Vct 25.02.2019 ���������� ����������� ����� ������� ���������� �������������
           -- �� �������� (������) ������� ���������� ����� ������� ���������� �������������
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- ������� �����
                                     , pn_rate => i_dpr_pecrent  -- ������ (�����������), ����������� �����
                                     , pd_reper_date => pd_evt_date -- ������� ����, �� ��������� � ������� ��������� ����� ������� ���������� �������������
                                     -- Vct 05.03.2019 , Vct case 28/08/2019 z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                     -- Vct 11.10.2019 z.196254 - ������ ������
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- ���������� ����� ������� ���������� ������������� � ������ �����.
                                     );

  --    END IF; -- Vct 25/07/2019 - ������� ������� �� ���� �������
  End;
  */
  ----------------------------------------------------------------
  /* Rem Vct 08.04.2021- TODO - �������� � ������������� ��������������� ��������� �� �����...
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ���� ������������ ������������� (403/404)
  -- TODO !!! - �������� ������������ ��������� ����.
  Begin

  -- Vct z.195805 - ����������� ������������ ������������� ������:
  -- �) ��� ��������� ����������� -- TODO - ���� �����������, ��� ��������� ����������� ������� ����������� ���������...
  -- �) ��� ������������ ������� �������
  -- b) ��� �������� ������� �� � ���� ��������� �������
    --
    -- Vct 09.10.2019
    IF b_flow_modified
       And (Not Coalesce(pb_correction_required, False)) --����  ����� ����������� ������� ���� �� ������������ ����� ��� ��� �������� ������� � ��������� ������� ������.
                                                         -- ������� �� ����� �� ���������, ���� ���� �������� ���������� ���������
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- ����� �����
                                      , p_DFrom => (d_prevevt_date + ci_One)
                                      , p_DTO  => pd_evt_date
                                        );
        Else
          m_clipay := Get_CliPay(p_AGRID =>  pn_agrId
                               , p_DFrom  => (d_prevevt_date + ci_One)
                               , p_DTO   => pd_evt_date
                                );
        END IF;
    END IF;

---------------------------------

    -- Vct 02.10.2019 z.195805 -- ������� ������������� ���������� ������������ �������������
    b_need_pareCorrection := pb_correction_required -- �������� ������������� �������� ������� ����� ������������ �������������
                       Or b_broken_schedule -- ������� ������ ������
                       OR (b_flow_modified
                          And m_clipay != cn_zero -- m_fact_revenue != cn_Zero -- z.19805 - ������ ������������ ������������� �������� ������ ��� ������� �������� ������ �� ����������� ����
                          -- z.196758 � �������������� ���� �� �������� ������� �� ���� ��������� 520�� �����...
                          -- (����� ���� �������� � ����������������� ��� �������...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                          --    And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate)
                              )
                          )
                          ;  -- ���������� ������ �� ���� ��������� �������, ���� �������� �� �������� ��������� ���������� ������� ���� � ������


   IF b_need_pareCorrection
     THEN

     -- Vct 17.10.2019
     IF b_flow_modified
       THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );

        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );

        END IF;

     END IF;


     ----------------------------------------------
-- Vct 25/07/2019 - ������� ������� �� ���� �������
-- Vct 25.02.2019 -- ����� ������������ ������������� ������� ������ ���� ������� ��� ���� �������
--    IF b_broken_schedule   -- Vct 26.02.2019
--      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      ------------------------
      -- ������������ ������������� ��� ����� �������.
      -- �������� ��� ����� ���������������, ��� � �������������� ������ ������ Vct 25.12.2018
      -------------------------------------------------------------------
      -- Vct 25.02.2019 ������ ������ ������� ������������ �������������
      -- ������ ����� ������� ��� ������� ����� ������� ���������� �������� ���������� �� ���� ��� �����
      -- ����� ����������� ������� ������������ ������������� � ������ ������� ���������� �������������

      -- m_pending_diff -- ����� ������� ���������� �������������
      -- ���� m_pending_diff ������������, �� �� ��������� ���������� �����, ���� ������������ �� ������

      -- m_revenue_diff Number; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
      -- m_pending_diff Number; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)

      -- ����� �������� ����� � cdbal ((153-152)-(150-151))
      -- �� �������������� ����
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- �������
                                                                          , pn_part => pn_part -- �����
                                                                          , pd_ondate => pd_evt_date - 1 -- ����
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- �������
                                                                        , pd_ondate => pd_evt_date - 1 -- ����
                                                                         );
      END IF;

      -- �������... TODO.. �������� ������� ������...
      vc_message := ' m_pending_current_rest before  ='||m_pending_current_rest
                  ||' m_revenue_diff='|| m_revenue_diff -- -- ����� ���������� ������������� � ������ �����.
                  ||' (m_pending_current_rest + m_revenue_diff)='||(m_pending_current_rest + m_revenue_diff)
                  ||' m_pending_diff='||m_pending_diff  -- ������ �������� ������� ���� ������������ �������������
                  ||' m_current_contribution = '||m_current_contribution
                  ||' d_curctr_Start='||to_char(d_curctr_Start,'DD.MM.YYYY')
                  ||' d_curctr_End='||to_char(d_curctr_End,'DD.MM.YYYY')
                  ||' m_accrued_revenue='||m_accrued_revenue
                  ;
      db_out('cd_dpr.mk_current_dpr_bnk_fv.2431'
             , vc_message
            );

      -- ��������� ����� ����������� ���������� �������������
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- � ����� ������� ������������� ��������� ������ �������� �������
      m_pending_diff := m_pending_diff + m_accrued_revenue;
      m_pending_evt_sum := cn_Zero;

      -- ��������� ����� ������������ �������������
      IF    m_pending_current_rest > cn_Zero -- ����� ��� �����
        And m_pending_diff <= cn_Zero        -- ����� ���� ����� �����
        THEN -- ���������� ������������� ����������� ����� � ���������� ����� ���� ��������
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ����� ��� ������
        And m_pending_diff >= cn_Zero         -- ������� ���� ������
        THEN -- ���������� ������������� ����������� ������ � ���������� ����� ���� ���������
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ����� ��� �����
        And m_pending_diff > cn_Zero         -- ������� ������
        THEN -- �������� ���� � ������ �� ������
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ����� ��� ������
         And m_pending_diff < cn_Zero        -- ����� ����� �����
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;
     ----- z.194702 Vct 26.07.2019
      i_evt_code := Case
                    When imorningPareSgn = 1 -- �������� ������� �� ����
                      Then
                      Case
                      When n_evt_sum > cn_Zero Then 401 --404 --401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 403 --402 -- 404 (���)
                      Else ci_Zero
                      End
                    When imorningPareSgn = -1 -- ��������� ������� �� ����
                      Then
                      -- TODO - ��� ��� ���������� ����������� �������� - ���� ���������.
                      Case
                      When n_evt_sum > cn_Zero Then 404 --401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (���)
                      Else ci_Zero
                      End
                    Else  -- ������� ������� �� ����
                      Case
                      When n_evt_sum > cn_Zero Then  401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (���)
                      Else ci_Zero
                      End
                    End;
      
      n_evt_sum := Abs(n_evt_sum);
      -- Vct 07.04.2021 
      -- �������� 401-404 ������ ������� 520 �����.
      -- �� ��� �������� ����� ��������������� m_520plan
      -- ��� ����������� ����� 409/410 ��������
      m4correction := Case i_evt_code
                      When 401 Then -n_evt_sum
                      When 404 Then -n_evt_sum
                      When 402 Then n_evt_sum
                      When 403 Then n_evt_sum    
                      End;  
      IF i_evt_code != ci_Zero
        And n_evt_sum != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_evt_sum
                                  , p_ccdeRem => '������� ������������ ������������� ��. �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 -- Vct 15.10.2018 z. -- ��� 403/404 �������� �� ������ ���� �������������
                                 , pc_Declarative => to_char(Null)
                                 );
        END IF;


--      END IF; -- Vct 25/07/2019 - ������� ������� �� ���� �������
    END IF; -- Vct 02.10.2019
  End;
  */
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ������� �� 520 ����� (������� 409 ��� ������������� ����, 410 - ��� �������������)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
    --  n_evt_sum Number;                -- ��������� ���������� ��� �������� ����� �������.

      -- Vct 26/02/2016
     -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum; -- rem Vct 07.04.2021
      n_evt_sum :=  n_dpr_full_money_prev - (m_520Plan + m4correction);
      
    -- Vct 22.06.2018
    -- 520 �������� ��������.
    -- ���������� ���������������
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (������) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code)
                  ||' n_dpr_full_money_prev='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
                  ||' m_dpr_after_curent='||cd_utl2s.num_to_str_dot( m_dpr_after_curent)
                  ||' (n_dpr_full_money_prev - m_dpr_after_curent)='||(n_dpr_full_money_prev - m_dpr_after_curent);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2524:'
               ,vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2524:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;

    -----------------------------
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
      -- TODO - ������ �����, ����� ���. ��������...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => 409
                                       , pi_evt_anti => 410
                                        );
      IF n_evt_sum2 != cn_Zero
        THEN
        i_evt_code := Case
                        When n_evt_sum > cn_Zero Then  410
                        When n_evt_sum < cn_Zero Then  409
                        Else ci_Zero
                      End;
      END IF;
    END IF;
    ----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (� ����:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2574:'
               , vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2574:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;
    -----------------------------
    ----- ��������� � �������� ������� ������ ��� �������� �� 409/410�� ��������, 520 ����
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => '������������� �������� ������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - ��������� �������� ����������� ������ z.190837
  -- � ����� ���������� ���������� � ��������/�����/����
  -- ����, ��� ��� �� ������ ����������...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - ��� ����� ���� �� ������������ ��� ���������� ����������� ������ ������ � �����-�� "��������� �������"
                                 -- ���� �� ����������...
      vc_message_text := SUBSTR(cd_errsupport.format_ora_errorstack(True),1,2000);
      vc_message_text := substr('cd_dpr.mk_current_dpr_bnk:ERROR:(pn_agrId='||pn_agrId
                             ||' pn_part='||pn_part
                             ||' pd_evt_date='||to_char(pd_evt_date,'DD.MM.YYYY')
                             ||'):'
                             ||vc_message_text, 1,2000);

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr_bnk.2621:'
                , vc_message
                );
      --    raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
      ELSIF bWrite_to_grpLog
        THEN
          -- TODO: ��������, ��� ����������� ������, ��������� ������� ��� ������ �� ������������ ������.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- �������� ��� ������...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;

 -------------------------------------------------------------------
 -- NB - ��� ��������� ��� ������ ������ �� ���� mk_current_dpr, �������������� �� ��������.
 --
 -- Vct 03.06.2019 - ������� ����������� �� ����� ��� ������,
 -- ����� ������� ����������� ���� �� ��� �� ���� ������ (153A/152P), ��� � ���������
  -- ��������� ��� �������������  � cdgrp.Recalc_CDD_Item302
 -- ��� ���������� ���������� �������� '1051' - ������� �����������
 -- ������������ ���� ��� ������ ���� ��� - �� ��������� �� 10.04.2018
 -- TODO - ������ ���. ��������, ��������� ��� ������������ � ���������� ���������.
 -- Vct 03.06.2019 - ���� ��� ������� ��� ����� ������ �����
 -- ����� � ��������� � ������ ����������� ������� �� ����� � ��� �� ���� ��������������� ������ (153A/152P)
 Procedure mk_current_dpr_inner( pn_agrId in Number -- �������
                               , pn_part in Integer -- ����� - ���� �� ����������...
                               , pd_evt_date in Date -- ����, � ������� ���������� ��������
                               , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                               , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 15.07.2019 IN OUT NOCOPY
                               --- z.196126 Vct 10.10.2019
                               -- �������� ������������� ��������� ������������� ������� �������� ����� ����������/������������ �������������
                               , pb_correction_required In Boolean --Default False      
                               --- Vct 15.05.2020
                               , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- ����� ����������� ���������� ����������                               
                               , pb_Write_to_grpLog In Boolean -- true - ��������� CDGRP.LOG_PUT(                               
                               -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                               )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
  i_dpr_pecrent Number; -- ������ �����������
  vd_dpr_enddate Date;  -- ���� ���������� �������� �����������
  -------------------
--  Vct 25/07/2019 - ������� ������� �� ���� �������
--  d_modification_start Date; -- ���� ������ �������� ������� �����������
------
-- Vct 25/07/2019 - ������� ������� �� ���� �������
--  b_broken_schedule Boolean; -- ���� ����� ������� � ������� ���.
                             -- ���� ���������, ���� ����������� ����� ������������ �������������
 -------------------
  -- Vct 02.10.2019 z.195805 - ���������� ��������� �������� ����� ������� � ������ ���
  -- Vct 25/07/2019 - ������� ������� �� ���� �������
  d_modification_start Date; -- ���� ������ �������� ������� ����������� -- Vct 25/07/2019 - ������� ������� �� ���� �������
  b_broken_schedule Boolean; -- ���� ����� ������� � ������� ���.        -- Vct 25/07/2019 - ������� ������� �� ���� �������
                             -- ���� ���������, ���� ����������� ����� ������������ �������������
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- ������� ������������� ���������� ������������ �������������
  b_deprecation_by_part Boolean; -- ���� ����������� �� ������
  m_current_contribution Number := cn_Zero; -- ����� ������� ���������� ������������� �������� � ���� ������� � �������� ������
  --
  m_accrued_revenue Number := cn_Zero; -- Vct 17.10.2019
  --
  m_clipay Number := cn_Zero;
  d_curctr_Start Date;  --| -- �������� �������, �� ������� ��������� ������� ����� m_current_contribution
  d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- ���� ������� ����������� �  pd_evt_date
  d_prevevt_date Date; -- ���� ���������� ����������� ������������ ����� ����������� ����� ������
  n_dbfixed_acc520 Number:= cn_zero; -- ������� ����� ����������� ����� ������, ��������������� � cdbal
                            -- ����� � ���� pd_evt_date. ��� ������� ������ �� �������� ��������
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; --

  -------------------
  n_dpr_revenue Number:= cn_zero; -- ����� �������� ������ �� �����������
  ----
  m_ovd_ondate Number:= cn_zero; -- ����� ��������� �� ������� ����.
  -- m_client_pay Number; -- ����� ����������� �������. -- ���� ��������� �� �������������
  m_fact_revenue Number:= cn_zero; -- ����� ���������� ���������� �������
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- ����� ����������� ����� ������, �� ������� ����� �������
                             -- 520 ���� �� ���������� ��������
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- ������������� ����� ����� ��� ��������� �������� ������
  vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- ��������� ������� ���������� ���� ���������� ��������� � �����
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
  n_evt_sum Number;                -- ��������� ���������� ��� �������� ����� �������.
  n_evt_sum2 Number;               -- ����� ������� � ������ ����� ������������ � ���� ��� ����� � cde
  m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
  --
  m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.

  m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)
  ----
  m_pending_evt_sum Number:= cn_zero; -- Vct 26.02.2019 -- ����� �������� ��� ������������ ������������� � ������ ���������� ������� �����������
  ----
  a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
                                        -- Vct 25.02.2019 - �������� �� bp_XNVP_cl
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
  vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
  bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- ��� ������ ����������� ������
  ------------------------------------------
  -- Vct 28.08.2019 - ������� ���������� ������� ���� (pd_evt_date) � �����
  b_flow_modified Boolean;
  -- Vct 23.09.2019
  i_evtdate_pos Pls_integer; -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- ������� ����� ��� ������ ����������� ����� ������
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- ���������� ����� ��� ������ ����������� ����� ������
  -----------------------------------        -- ��� ������������� � ������ ����� �������� ��������
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- ��������� ��������� ������ � a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   --bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   bWrite_to_grpLog := pb_Write_to_grpLog;
   vc_dpr_code := pc_dpr_code; -- Vct 15.04.2020
   
  /* rem Vct 15.05.2020 - ���������� � mk_current_dpr
   ---------------------------------
   -- ������� ������������� ������� ����������� ( � ������ ��������� ���������)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ������ �������, ���� �� ���������� ������� ������� ����������� �� ��������
      vc_message:='�� �������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����������� �� ������� (�� ��������� ������� ������� ����������� � �������� ��������)';
     -- �������� ��� ��������
     IF bWrite_to_grpLog
       THEN -- ����������� ��� �������� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.80:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr.80:'
              , pc_text => vc_message
              );
     END IF;
     Return;
   ELSIF cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
         And (Not cd_dpr_utl.has_deprecation_mark( pn_agrid => pn_agrid
                                                 , pn_part => pn_part)
             )
       THEN -- ���������� ������� ������� ����������� �� ������, �� ��� �������� ������� ����������� �� �����
      vc_message:='������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����� <'||pn_part
                         ||'> �� ���������� ������� ������ ����������� ��� �����.';

     IF bWrite_to_grpLog
       THEN -- ����������� ��� ����� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.82:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr.82:'
              , pc_text => vc_message
              );
     END IF;
     Return;

   END IF;
   db_out(pc_prefix => 'cd_dpr.mk_current_dpr.2501:'
              , pc_text => '  pn_agrid='|| pn_agrid||' pn_part='||pn_part
                         ||' pd_evt_date='||pd_evt_date
                         ||' vc_dpr_code='||vc_dpr_code
              );
                 
   */
  -- Vct 02.10.2019
  -- ���� ��������� ����,  ������ �������� ����������� �� �������� ����������� � ���������������...
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- ��������� ������ � ���� ���������� �����������
  Begin

    -- �������� ������ �����������.
    /* rem Vct 14.02.2019
    -- TODO - ����� ���� �� ������������ ����������� ������ ������ ����������� �� �������!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- �������
                                        , pd_evt_date -- r.DCDASIGNDATE -- ����, �� ������� �������� ������ �����������, ���� �� ���� ����������...
                                         )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- �������� ������ �����������.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- ����� �����
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- ������� ���� ���������� ����������� �� ��������/�����, ���� �����������
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- �������
                                                   , pn_part => pn_part -- �����
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ������ �������:
    -- ���� ������ �� ������������ (��� ����������) ��� ����� ���� (����������� ����������)
    -- ��� ���� ���������� ����������� ������������ pd_evt_date,
    -- �� ��������� ������ ���������
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : ����������� ��������� ��� �� ����������';

        IF bWrite_to_grpLog
          THEN
          CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_inner.2549:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
        ELSE
          db_out('cd_dpr.mk_current_dpr.2549:'
                 , vc_message
                );
        END IF;
        RETURN;
    END IF;
    vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code;
    db_out('cd_dpr.mk_current_dpr.2561:'
                 , vc_message
                );
    -------------------------------------------------
  End;
  ---------------------------------------------------------
  -- �������� ������� �� ����� 520 � ���� ��� �������������
  Begin
    -------------------------------------------------
    -- �������� ������� ����� 520 -------------------
    -- �������� ������� � ��� ���� �� ����� ����������� ����� ������
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c ������ ����������� �� ������
                              , PART => case when b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- ������� ��� ����������� ��������� �����������
           -- ���� ���-�� ������ ��������� �� ���������.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- �������� ������� ����� 520 �� ���������� ����  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c ������ ����������� �� ������
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- ���� �������������� �����������
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- � pd_evt_date ����������� �� �����������.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '����. �������-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' ��������� ������='|| case b_dpr_was_today When True Then '��' Else '���' end
               ||' n_dbfixed_acc520:='||cd_utl2s.num_to_str_dot(n_dbfixed_acc520)
               ||' vc_dpr_code='||vc_dpr_code
               ||' pn_part='||pn_part;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.137:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.137:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    ---------------------------------------------------------
  End;
  -----------------------------------------------------------
  -- ����������, ��� �� ���� ������� ��������/��������� � ������� ���
  -- Vct 02.10.2019 z.195805 - ���������� ��� ��������� ���������� � ����� �������
  -- Vct 25/07/2019 - ������� ������� �� ���� �������
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- ������������� ��������
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- ����� �����,
                                  , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- ������� ����, �� ������� ���������� ����� �����������
                                  , pb_isdbms => pb_isdbms                       -- True - ����������� dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- ���� ������ �������� ��
                                  );
    ----
    -- ��������� TODO !!! - ����� ������������ ��������� ������� �������,
    -- ������ ���� �� ��������� � ����, �������� �� ����.
    -- ��� ��������� ��������� � ���������, ����� ������� ���������
    -- �� ��������� ����� vd_dpr_enddate � pd_evt_date ���� ��� �� �����.
    -- � ������ �������,��� ��� � �� �������� �����.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���� ������ ������� d_modification_start:='||fmt_date_out(d_modification_start)
               ||' b_broken_schedule:='|| case b_broken_schedule When True Then 'True' Else 'False' end;
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.302:'
               , vc_message
              );
    ELSIF bWrite_to_grpLog
      THEN
      CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.302:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;

  ---------------------------------------------------------
  -- ���������� ����� �������� ���������������� ������ �� ������
  Begin
    /* -- rem Vct 22.11.2018
    -- ��������� �������� ������ �� �����������
    n_dpr_revenue := calc_DPR_Revenue( pf_rate_in => i_dpr_pecrent -- ��������� %%� ������ (����� - ������ �����������)
                                      , pd_startdate => d_prevevt_date -- ���� ������ ���������
                                      , pd_enddate => pd_evt_date   -- ���� ���������� ���������
                                      , ppf_pmt => n_dpr_full_money_prev    -- �����, �� ��������� � ������� ����������� �����
                                      );
    */
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ������ �������������� � ������ ���� ������ ������������ ��� �������� � �����.
    -- ������� �� �������� ����� � ������ ������� �����������.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- �������, ��� �������� ���������� ����������� ������
                               , pd_evtDate => pd_evt_date  -- "���� �������" ��� "������� ����", ��������, ����� ������ ����� ��������� �  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- ��������� %%� ������ (����� - ������ �����������)
                               , pd_startdate => d_prevevt_date -- ���� ������ ���������
                               , pd_enddate => pd_evt_date   -- ���� ���������� ���������
                               , ppf_pmt => n_dpr_full_money_prev    -- �����, �� ��������� � ������� ����������� �����
                                                      -- �� �������� �������������, ���� �������� ���������� �������
                               , pb_isdbms => pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ������� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' � d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' �� pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' � ����� n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' �� ������ i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.595:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.595:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
    -- TODO - ����� �������� ���������� !!! ����� ������ ����� ����������� ����� ����
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                              , pi_part => pn_part -- �����
                              , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- ����, �� ������� ������ ���� 400� ��������
    ----- ��������� � �������� ������� ������ ��� �������� �� 400�� ��������, 521 ����
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => 400   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ���� 521)
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => '������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' ���������� � ���� ������� �� ����� (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
    END IF;

    IF pb_isdbms AND Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.618:'
               , vc_message
              );
    ELSIF  bWrite_to_grpLog
      THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.618:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  ---------------------------------------------------------
    ---------------------------------------------------------
  -- �������� �������� � ������������ ���� ���������� �������������
  ---------------------------------------------------------
  Begin
  ---------------------------------------------------------
  /* -- ���� ���������� �� �������������
  -- �������� ����� ���������� �������� �� ������ d_prevevt_date - pd_evt_date
  Begin
    -- ��� ����� � �������������� ������ ����������� ��� ���������� ����������� ����� ������
    -- ���� ���������� �� �������������
    m_client_pay := CDSTATE2.Get_ClientPay_LPCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
  End;
  */
  ---------------------------------------------------------
    -- �������� ����� ����������� (!!!) --���������� ���������� ������� �� ������
    Begin
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- ��� ��� ������ �� ������.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- ����� �����
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- � ����� �� ��������
         m_fact_revenue := CDSTATE2.Get_Profit_PCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
      END IF;
    End;
    ----------------------------------------------------------
    -- ��������� ����� �������� �� ���������� ���������� �������������
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
      m_revenue_diff := n_evt_sum; -- ����� ���������� ������������� � ������ �����.
      i_evt_code := Case
                      When n_evt_sum > 0 Then 405
                      When n_evt_sum < 0 Then 406
                      Else 0
                    End;
      n_evt_sum := Abs(n_evt_sum);
      ----------------------------------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (��������������� �����) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (����������� �����) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
                    ||' (n_evt_sum):='||cd_utl2s.num_to_str_dot(n_evt_sum)
                    ||' i_evt_code :='||i_evt_code;
      END IF;
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr.865:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.865:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -----------------------------------------------------
      -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
      -- ����� ������������ ����� ��������� ������������� ��������
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
        -- TODO - ������ �����, ����� ���. ��������...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                         , pi_part => pn_part -- �����
                                         , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => 405
                                         , pi_evt_anti => 406
                                          );
        IF n_evt_sum2 != cn_Zero
          THEN
          i_evt_code := Case
                          When n_evt_sum > cn_Zero Then  405
                          When n_evt_sum < cn_Zero Then  406
                          Else ci_Zero
                        End;
        END IF;
      END IF;
      ----------------------------------------------------
      IF i_evt_code != ci_Zero
        And n_evt_sum2 != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                   , pa_evt_queue => pa_result
                                   , p_ncdeAgrid => pn_agrID      -- �������
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                   , p_icdeType => i_evt_code   -- ��� ������� - 400 - ���������� ������ �� ����������� (������ �� ���� 521)
                                   , p_dcdeDate => pd_evt_date -- ���� �������
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => '������������� �����. ������� �������� '||cd_utl2s.num_to_str_dot(pn_agrId)  -- ����������� � ��������  -- ����������� � ��������
                                   -- Vct 15.10.2018 z. -- ��� 405/406 �������� �� ������ ���� �������������
                                   , pc_Declarative => to_char(Null)
                                   );
      END IF;
      -----------------------------------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message :=  ' n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                ||' i_evt_code :='||i_evt_code;
      END IF;
      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr.924:'
                , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.924:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
    End;
    -----
  End;
  ---------------------------------------------------------
  -- ��������� ����� ��� ������ �� ���������� ������� �� 520 �����
  -- ��������� ����� ������������ ������������� ��� ����� �������
  ---------------------------------------------------------
  ---------------------------------------------------------
  ---- ���������� ����� ��������� � ������� ���
  Begin
    ------- ������ ���������, ������������ �� ������� ���� (���� ������� ��������� �������� ���)
    /*
    ----- rem vct 14.02.2019
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount( pn_AgrID_in => pn_agrId -- ������������� ��������
                                             , pd_onDate => pd_evt_date  -- (d_evtDate - ci_One) -- �� ���� -- ����, �� ������� ������������ ������� (�� �����)
                                             );
    ------
    */
    ---- Vct 14.02.2019
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- ������������� ��������
                                                    , pn_part => pn_part -- �����
                                                    , pc_code => vc_dpr_code -- ��� ������� ������� ����������� - �� �����, �������� ��� �� �������
                                                    , pd_onDate => pd_evt_date -- ����, �� ������� ������������ ������� (�� �����)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.572:'
               , '(���������) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.572:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- �������� ������������� ����� ����� ��� ��������� �������� ������,
  -- ���� ������, ����� �������������� "����������� �����"
  -- + �������� ������ ���������� ����, � ������� ����� ��������� ����� ���������
  Begin
      --------------------------------------------
      -- �������� �����, ��������������� ���������� ���������
      --------------------------------------------
      -----
      -- �������� ������������� ����� �����, ������������ ���������������� ������ ����������� ������
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- �������� ��������� ������� ����� ��������� � ������.
      -- ���� ����� ������� �� ����� �� ���� ������ �� ��������.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- ���������� ����� ������� ����������� ����� ������,
  -- � ����� ����������� ����� ������ �� ����������� �������
  -- ����� ����� ��������� �������� �� ����� ����������� ����� ������
  -- � ����� ������� ������������ �������������
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- ����� ����������� ����� ������, �� ������� ����� �������
      --                       -- 520 ���� �� ���������� ��������
      --  m_dpr_after_prev Number;   -- ����� ����������� ����� ������ ��� ����� �������
      -----------------------------------------------------------------------------------
      -- ����������� 23.01.2019 - � ��������...
       --   COMMIT WORK WRITE BATCH NOWAIT;
      bp_XNPV_lc( pn_agrId => pn_agrId -- �������
                -- Vct 14.02.2019
                , pn_part => pn_part -- ����� �����,
                , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                --
                , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                , pc_odvclc_code  => vc_odvclc_code  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                , pd_flow_date => pd_evt_date -- ����, �� ������� ������� �������� �����
                , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                , pm_ovd_sum => m_ovd_ondate -- ����� ���������, ������� ���������� ������ ��� �������
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- ������� ����� ��� ������ ����������� ����� ������
                -- Vct 28.08.2019 - ������� ���������� ������� ���� (pd_evt_date) � �����
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
                );
      ---------------------------
      -- ���� ��� ���� �������, ���������� ����� ����� ������ �� ������� �������
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

-- Vct 25/07/2019 - ������� ������� �� ���� �������
--      IF b_broken_schedule
--        THEN -- ��� ���� �������
           -- Vct 25.02.2019 ���������� ����������� ����� ������� ���������� �������������
           -- �� �������� (������) ������� ���������� ����� ������� ���������� �������������
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- ������� �����
                                     , pn_rate => i_dpr_pecrent  -- ������ (�����������), ����������� �����
                                     , pd_reper_date => pd_evt_date -- ������� ����, �� ��������� � ������� ��������� ����� ������� ���������� �������������
                                     -- Vct 05.03.2019 , Vct 28.08.2019 case z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                    -- Vct 11.10.2019 z.196254 - ������ ������
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- ����� � ���� �� ��������� � pd_reper_date ��� ����������� ������������� (1 - ����, 0 - ������� pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- ���������� ����� ������� ���������� ������������� � ������ �����.
                                     );

       /* -- rem Vct 26.02.2019
           -- ��������� ����� ����� ������ �� ������� �������
        bp_XNPV_lc( pn_agrId => pn_agrId -- �������
                  -- Vct 14.02.2019
                  , pn_part => pn_part -- ����� �����,
                  , pc_dpr_code => vc_dpr_code --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
                  --
                  , pc_extflowsqlid => vc_extflowsqlid -- ������������� ����� ����� ��� ��������� ������
                  , pc_odvclc_code  => vc_odvclc_code  -- ��������� ������� ���������� ���� ���������� ��������� � �����
                  , pi_dpr_pecrent => i_dpr_pecrent -- ������ �����������
                  , pd_flow_date => (d_modification_start - 1) -- ����, �� ������� ������� �������� �����
                  , pd_evt_date  => pd_evt_date -- ���� �������� �������.
                  , pm_ovd_sum => m_ovd_ondate -- ����� ���������, ������� ���������� ������ ��� �������
                  , pm_sum_after_mn => m_dpr_after_prev
                  );
                  -- ���������� ����������� ����� ������� ���������� �������������
      ELSE
        -- ����� ������� �� ����.
        -- ����� �� ����������� ������� ������� ������ ������� ����� ����� ������
        m_dpr_after_prev := m_dpr_after_curent;
       */
--      END IF;

  End;
  ----------------------------------------------------------------
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ���� ������������ ������������� (403/404)
  -- TODO !!! - �������� ������������ ��������� ����.
  Begin
    -- Vct 25.02.2019 -- ����� ������������ ������������� ������� ������ ���� ������� ��� ���� �������
-- Vct 25/07/2019 - ������� ������� �� ���� �������
--    IF b_broken_schedule   -- Vct 26.02.2019
---      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      -------------------------------------------------------------------
    -- Vct 02.10.2019 z.195805 -- ������� ������������� ���������� ������������ �������������
    -- Vct 09.10.2019
    IF b_flow_modified
        And (Not Coalesce(pb_correction_required, False)) --����  ����� ����������� ������� ���� �� ������������ ����� ��� ��� �������� ������� � ��������� ������� ������.
                                                          -- ����� �� ����� �� ���������, ���� ���� �������� ���������� ���������
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- ����� �����
                                      , p_DFrom => (d_prevevt_date + ci_One)
                                      , p_DTO  => pd_evt_date
                                        );
        Else

          m_clipay := Get_CliPay(p_AGRID =>  pn_agrId
                               , p_DFrom  => (d_prevevt_date + ci_One)
                               , p_DTO   => pd_evt_date
                                );
        END IF;
    END IF;

    b_need_pareCorrection :=  pb_correction_required -- �������� ������������� �������� ������� ����� ������������ �������������
                       OR  b_broken_schedule -- ������� ������ ������
                       OR (b_flow_modified  -- ���������� ������ �� ���� ��������� �������, ���� �������� �� �������� ��������� ���������� ������� ���� � ������
                          And m_clipay != cn_Zero -- m_fact_revenue != cn_Zero -- z.19805 - ������ ������������ ������������� �������� ������ ��� ������� �������� ������ �� ����������� ����
                          -- z.196758 � �������������� ���� �� �������� ������� �� ���� ��������� 520�� �����...
                          -- (����� ���� �������� � ����������������� ��� �������...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                            --  And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate) -- !!! Vct 09.12.2020 -- ������� ������ �� �������� 22527/528 (���������, ����� ������� ����������� ��� ������� � ����� ��� �������.
                              )
                          );


   IF b_need_pareCorrection
     THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );
          -----
          /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- �������
                                               , pn_part => pn_part
                                              , pc_RT => 'T' -- ��� ������� %%, ��� �������� �������� �����
                                                             -- ���� �������, ��� ������ ������ �� T? ������ ���������
                                              , pd_onDate => pd_evt_date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                                              );
         */
        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- ������������� ��������
                                            , pd_onDate => pd_evt_date  -- ����, �� ������� ������ ���� �������� �������
                                             );
         /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- �������
                                                , pc_RT => 'T' -- ��� ������� %%, ��� �������� �������� �����
                                                               -- ���� �������, ��� ������ ������ �� T? ������ ���������

                                                , pd_onDate => pd_evt_date -- ������� ����, ��������� ��������������� �������� ���������� ���������
                                                );
        */
        END IF;

       -------------------
/*   -- rem Vct 11.10.2019 z.196254
     ----------------------------
     -- Vct z.195805 02.10.2019
     -- ��������� ����� ���������� ������������� � ��������� ����� ���������� ��������� ������
     IF b_flow_modified
       THEN
         -- ���������� �������� ����� ������� ���������� �������������
       --  d_curctr_Start Date;  --| -- �������� �������, �� ������� ��������� ������� ����� m_current_contribution
        IF i_evtdate_pos > 1 -- ������� ��������� ������, ��������������� ���� �������� ������� pd_evt_date
          THEN
           d_curctr_Start :=   a_potok_xnpv(i_evtdate_pos - 1).ddate;
           d_curctr_Start := d_curctr_Start + 1; -- ��������� ���� �� "�����������" �������� � ������
        Else
           d_curctr_Start := cd_chdutils.cd_zero_cd_date; -- �� ������ �����
        END IF;
        ----
        d_curctr_End := pd_evt_date - 1;    -- ���������� ���� �� ��������� �� ���� ���������� ���
        ----
        IF  b_deprecation_by_part
           THEN
           -- ������� �� �����
           m_current_contribution := get_cdeDprPrcSubSum_MfoSchm( pd_dateStart => d_curctr_Start -- ������ ���������� ���������
                                                               , pd_dateEnd => d_curctr_End -- ���������� ���������
                                                               , pn_agrId => pn_agrid -- ������������� ��������
                                                               , pn_part => pn_part -- ����� �����
                                                               );
         ELSE
           -- �� �������� � �����
           m_current_contribution := get_cdeDprPrcSubSum_MfoSchm( pd_dateStart => d_curctr_Start -- ������ ���������� ���������
                                                               , pd_dateEnd => d_curctr_End -- ���������� ���������
                                                               , pn_agrId => pn_agrid -- ������������� ��������
                                                               );
        END IF;
     END IF;
*/
     ----------------------------------------------
      -------------------------------------------------------------------
/* rem Vct 02.10.2019
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' ���� ������� � ���������� ������ ������ m_dpr_after_curent:='||cd_utl2s.num_to_str_dot(m_dpr_after_curent)
                    --||' m_dpr_after_prev:='||cd_utl2s.num_to_str_dot(m_dpr_after_prev)
                    --||' delta:='|| cd_utl2s.num_to_str_dot(m_dpr_after_curent - m_dpr_after_prev)
                    ||' m_pending_diff='||cd_utl2s.num_to_str_dot(m_pending_diff)
                    ;
      END IF;

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr.670'
                 , vc_message
                );
      ELSIF bWrite_to_grpLog
        THEN
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.670:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
*/
      -------------------------------------------------------------------
      -- ������������ ������������ ��� ����� �������.
      -------------------------------------------------------------------
      -- ��� ������, ��������� ���� m_dpr_after_curent - m_dpr_after_prev
      -- n_evt_sum := m_dpr_after_curent - m_dpr_after_prev; -- rem Vct 26.02.2019

      ----------------------------------------------------------
      -- Vct 25.02.2019 ������ ������ ������� ������������ �������������
      -- ������ ����� ������� ��� ������� ����� ������� ���������� �������� ���������� �� ���� ��� �����
      -- ����� ����������� ������� ������������ ������������� � ������ ������� ���������� �������������

      -- m_pending_diff -- ����� ������� ���������� �������������
      -- ���� m_pending_diff ������������, �� �� ��������� ���������� �����, ���� ������������ �� ������

      -- m_revenue_diff Number; -- Vct 25.02.2019 - ����� ���������� ������������� ������������ ��� � ������ �����
      -- m_pending_diff Number; -- Vct 25.02.2019 - ���������� ����� ������� ���������� ������������� �� ����� �������.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - ����� ������� �� �������������� ���� �������� ����� ����� ������/������� (������������ �������������)

      -- ����� �������� ����� � cdbal ((153-152)-(150-151))
      -- �� �������������� ����
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- �������
                                                                          , pn_part => pn_part -- �����
                                                                          , pd_ondate => pd_evt_date - 1 -- ����
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- �������
                                                                        , pd_ondate => pd_evt_date - 1 -- ����
                                                                         );
      END IF;

      -- �������... TODO.. �������� ������� ������...
      vc_message := ' m_pending_current_rest before  ='||m_pending_current_rest
                  ||' m_revenue_diff='|| m_revenue_diff
                  ||' (m_pending_current_rest + m_revenue_diff)='||(m_pending_current_rest + m_revenue_diff)
                  ||' m_pending_diff='||m_pending_diff
                  ||' m_current_contribution='||m_current_contribution
                  ||' d_curctr_Start='||to_char(d_curctr_Start,'DD.MM.YYYY')
                  ||' d_curctr_End='||to_char(d_curctr_End,'DD.MM.YYYY')
                  ||' m_accrued_revenue='||m_accrued_revenue
                  ;
      db_out('cd_dpr.mk_current_dpr.3063'
             , vc_message
            );

      -- ��������� ����� ����������� ���������� �������������
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- � ����� ������� ������������� ��������� ������ �������� �������
      m_pending_diff := m_pending_diff + m_accrued_revenue;
      ------------------
      /* rem Vct 11.10.2019 z.196254
      --------------------------------
      -- 05.09.2019 Vct - �������� ���������� ������������� �������� ��� �� ������ ������� �������������
      -- ��� ������ ����� � ������� ����� ���� ������������� ��������� ������������ ������
      -- (�������������� ��� ���������� ��������� ������ � ������� ���.)
      IF b_flow_modified
        THEN
        -- 02.10.2019 z.195805 ����� ��� ������ ����� ���������� �������������
        m_pending_current_rest := m_pending_current_rest - m_revenue_diff - m_current_contribution;
        --------------------------------------------------------------------------
       --  m_pending_diff := m_pending_diff - m_revenue_diff; -- rem Vct
        ----
       -- db_out('cd_dpr.mk_current_dpr.3156', 'm_pending_diff='||m_pending_diff );
      END IF;
      */

      m_pending_evt_sum := cn_Zero;

      -- ��������� ����� ������������ �������������
      IF    m_pending_current_rest > cn_Zero -- ����� ��� �����
        And m_pending_diff <= cn_Zero        -- ����� ���� ����� �����
        THEN -- ���������� ������������� ����������� ����� � ���������� ����� ���� ��������
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ����� ��� ������
        And m_pending_diff >= cn_Zero         -- ������� ���� ������
        THEN -- ���������� ������������� ����������� ������ � ���������� ����� ���� ���������
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ����� ��� �����
        And m_pending_diff > cn_Zero         -- ������� ������
        THEN -- �������� ���� � ������ �� ������
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ����� ��� ������
         And m_pending_diff < cn_Zero        -- ����� ����� �����
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;

      i_evt_code := Case  -- Vct 25.12.2018 - ������ �������� � ����� � ������ ��� ����� �� ����������                      --
                      When n_evt_sum > cn_Zero Then 401 --- 403 (���)
                      When n_evt_sum < cn_Zero Then 402 --404 (���)
                      Else ci_Zero
                    End;

/*
      i_evt_code := Case  -- Vct 25.12.2018 - ������ �������� � ����� � ������ ��� ����� �� ����������
                    --  When n_evt_sum > cn_Zero  Then 401 --  401 --403 (���)
                    --  When n_evt_sum < cn_Zero  Then 402  --404 (���)
                      -- Vct 10.04.2019 ���-�� ����������� � ������������ ��������, ����������� ������� 401/402 ������� (�����������)
                      When n_evt_sum > cn_Zero And sign(m_pending_current_rest) >= cn_Zero Then 401 --  401 --403 (���)
                      --When n_evt_sum > cn_Zero And sign(m_pending_current_rest) < cn_Zero Then 402 --  401 --403 (���)

                      When n_evt_sum < cn_Zero And sign(m_pending_current_rest) >= cn_Zero  Then 402  --404 (���)
                      --When n_evt_sum < cn_Zero Then 402 --  --404 (���)
                      Else ci_Zero
                    End;
 */
      n_evt_sum := Abs(n_evt_sum);

      IF i_evt_code != ci_Zero
        And n_evt_sum != cn_Zero
        THEN -- Vct 15.07.2019
          make_next_out_array_element(  icurPos => i_out_index
                                    , pa_evt_queue => pa_result
                                    , p_ncdeAgrid => pn_agrID      -- �������
                                    , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                    , p_icdeType => i_evt_code   -- ��� �������
                                    , p_dcdeDate => pd_evt_date -- ���� �������
                                    , p_mcdeSum => n_evt_sum
                                    , p_ccdeRem => '������� ������������ ������������� ��. �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                   -- Vct 15.10.2018 z. -- ��� 403/404 �������� �� ������ ���� �������������
                                   , pc_Declarative => to_char(Null)
                                   );
      END IF;

--     END IF;  -- Vct 25/07/2019 - ������� ������� �� ���� �������
    END IF; -- Vct 02.10.2019 z.195805
  End;
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- ��������� �������� �� ��������� ������� �� 520 ����� (������� 409 ��� ������������� ����, 410 - ��� �������������)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- ��� ������������� �������
    --  n_evt_sum Number;                -- ��������� ���������� ��� �������� ����� �������.


--    IF b_broken_schedule   -- Vct 26.02.2019
--      -- Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN -- !!!! ����� ������� ���-�� �������!
      -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + (m_dpr_after_curent - m_dpr_after_prev); -- ������ ����� ��� ����� ������������ �������������
      -- Vct 26/02/2016
       n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum;
      -- Vct 05.10.2019
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);

--    ELSE
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    END IF;

    -- Vct 22.06.2018
    -- 520 �������� ��������.
    -- ���������� ���������������
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (������) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code)
                  ||' n_dpr_full_money_prev='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
                  ||' m_dpr_after_curent='||cd_utl2s.num_to_str_dot( m_dpr_after_curent)
                  ||' (n_dpr_full_money_prev - m_dpr_after_curent)='||(n_dpr_full_money_prev - m_dpr_after_curent);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr.730:'
               ,vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.730:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;
    -----------------------------
    -- ������������� ����� �������� � ������ ����� ������������� � cde ��������
    -- ����� ������������ ����� ��������� ������������� ��������
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- ����� ����� �������� n_evt_sum ����������� ������ ����, �� � ���� ����� ���������� ����� ���-�� ������...
      -- TODO - ������ �����, ����� ���. ��������...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- ��� ��������
                                       , pi_part => pn_part -- �����
                                       , pd_onDate => pd_evt_date -- ����, � ������� ���������� ���������
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => 409
                                       , pi_evt_anti => 410
                                        );
      IF n_evt_sum2 != cn_Zero
        THEN
        i_evt_code := Case
                        When n_evt_sum > cn_Zero Then  410
                        When n_evt_sum < cn_Zero Then  409
                        Else ci_Zero
                      End;
      END IF;
    END IF;
    ----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' �������. ������� ��. (� ����:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
                  ||' i_evt_code:='||cd_utl2s.num_to_str_dot(i_evt_code);
    END IF;
    IF pb_isdbms And Not bWrite_to_grpLog
        THEN
        db_out('cd_dpr.mk_current_dpr.772:'
               , vc_message
              );
     ELSIF bWrite_to_grpLog
       THEN
       CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.772:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
     END IF;
    -----------------------------
    ----- ��������� � �������� ������� ������ ��� �������� �� 409/410�� ��������, 520 ����
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- �������
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - ����� ���������  -- �����
                                  , p_icdeType => i_evt_code   -- ��� �������
                                  , p_dcdeDate => pd_evt_date -- ���� �������
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => '������������� �������� ������� ����� ����������� �� �������� '||cd_utl2s.num_to_str_dot(pn_agrID)  -- ����������� � ��������  -- ����������� � ��������
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - ��������� �������� ����������� ������ z.190837
  -- � ����� ���������� ���������� � ��������/�����/����
  -- ����, ��� ��� �� ������ ����������...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - ��� ����� ���� �� ������������ ��� ���������� ����������� ������ ������ � �����-�� "��������� �������"
                                 -- ���� �� ����������...
      vc_message_text := SUBSTR(cd_errsupport.format_ora_errorstack(True),1,2000);
      vc_message_text := substr('cd_dpr.mk_current_dpr:ERROR:(pn_agrId='||pn_agrId
                             ||' pn_part='||pn_part
                             ||' pd_evt_date='||to_char(pd_evt_date,'DD.MM.YYYY')
                             ||'):'
                             ||vc_message_text, 1,2000);

      IF pb_isdbms And Not bWrite_to_grpLog
        THEN
          db_out('cd_dpr.mk_current_dpr.1227:'
                , vc_message
                );
      --    raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
      ELSIF bWrite_to_grpLog
        THEN
          -- TODO: ��������, ��� ����������� ������, ��������� ������� ��� ������ �� ������������ ������.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- �������� ��� ������...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;


 -------------------------------------------------------------------
  -- ��������� ��� �������������  � cdgrp.Recalc_CDD_Item302
 -- ��� ���������� ���������� �������� '1051' - ������� �����������
 -- ������������ ���� ��� ������ ���� ��� - �� ��������� �� 10.04.2018
 -- TODO - ������ ���. ��������, ��������� ��� ������������ � ���������� ���������.
 -- Vct 03.06.2019 - ���� ��� ������� ��� ����� ��������������� ��������,
 -- ����� ��������� ����������� ������� �� ���� ��������������� ������ (153A/152P),
 -- � ������� �� ���� (151A/150P)
 -------------------------------------------
 /*
  Vct 15.04.2020 
  ��������:
  ����� ���� �������� � ����������� �� ����������� ��������� � cdgrp 
  ����� ����������� ����� ������ ���� - ��� ������ ����� 
  -- ��� ����� ���������
  -- �) � ���������� ������� ��������� ��� ������ �� ������
  -- �) (������������) � ������������ ��������������� �������� ��� ������ ����������� � ����� �� ��������.
  ---
  ��� ��������� ����������� ������������� ����� ������ �� ������������ ��������
  ---
  ����
  �) (����� ����) ������������ � ��� ������� �����������
  �) �������� ������ �� ���������� ������� ���������
 */
 Procedure mk_current_dpr( pn_agrId in Number -- �������
                         , pn_part in Integer -- ����� - ���� �� ����������...
                         , pd_evt_date in Date -- ����, � ������� ���������� ��������
                         , pb_isdbms in Boolean -- ������� ������ ���������� � dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + nocopy
                         --- z.196126 Vct 10.10.2019
                         -- �������� ������������� ��������� ������������� ������� �������� ����� ����������/������������ �������������
                         , pb_correction_required In Boolean Default False
                         ---
                        -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                         )
 Is
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  ������� ������� ����������� �� �������� '0' - ���� �� ������� '1' - ������� � ����� �� ��������, 2 - ������� � ������� ������
  ------------------------------------------
  vc_message cd_types.T_MAXSTRING;
  --vc_message_text cd_types.TErrorString;  -- ��� ������ ��������� � ����������� ������
  ------------------------------------------   
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- ������������� "���� ������" ��� ��������� �����������
  vn_TypPrtkl   NUMBER := NULL; -- "��� ���������" ��� CDGRP.LOG_PUT, ��� Null ��������������� � �������� ���������� �� �������� ������.
  bWrite_to_grpLog Boolean := False; -- ��� ��������� ��������� ������������ LOG_PUT
  ------------------------------------------  
  a_empty_result cd_dpr.T_CDE_CALL_QUEUE_PLTAB; -- ������ ������ ��� ������ �������� � ������ ������� ��������
 Begin
   
   -- �������� ������ ������ � ���������� ����� � ��� ������ ������ ��������
   Check_DBMSOUT_Job_Mode();
   pa_result := a_empty_result; -- ����� ��������, ��������� ������, ����� ����� ��� ������� ��������� ��������� 
   
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
  -------------------------------------------
  -------------------------------------------
   -- ������� ������������� ������� ����������� ( � ������ ��������� ���������)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);
   
   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ������ �������, ���� �� ���������� ������� ������� ����������� �� ��������
      vc_message:= '�� �������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����������� �� ������� (�� ��������� ������� ������� ����������� � �������� ��������)';
     -- �������� ��� ��������
     IF bWrite_to_grpLog
       THEN -- ����������� ��� �������� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.4160:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr_bnk.4160:'
              , pc_text => vc_message
              );
     END IF;
     Return;
   ELSIF cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
         And (Not cd_dpr_utl.has_deprecation_mark( pn_agrid => pn_agrid
                                                 , pn_part => pn_part)
             )
       THEN -- ���������� ������� ������� ����������� �� ������, �� ��� �������� ������� ����������� �� �����
      vc_message:='������� <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> ����� <'||pn_part
                         ||'> �� ���������� ������� ������ ����������� ��� �����.';

     IF bWrite_to_grpLog
       THEN -- ����������� ��� ����� �� �������
         CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.4186:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
       ELSE
        db_out(pc_prefix => 'cd_dpr.mk_current_dpr.4186:'
              , pc_text => vc_message
              );
     END IF;
     Return;

   END IF;

   db_out(pc_prefix => 'cd_dpr.mk_current_dpr.4186:'
        , pc_text => '  pn_agrid='|| pn_agrid||' pn_part='||pn_part
        ||' pd_evt_date='||pd_evt_date
        ||' vc_dpr_code='||vc_dpr_code
              );
 ---------------------------------  
   -- ��������� ����� ��������� ��������� � ���������� ����������
   Setup_currentdpr_gchache(
                           pn_agrID => pn_agrid -- �������
                         , pc_dprcode => vc_dpr_code -- ��� ������� ������� �����������: 0 - �� �������, 1 ������� �� ��������, 2 ������� �� ������
                         , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������   
                          );
   --------------------------  
   IF Not Calc520_From_RecordedIncome
     Then                     
     If isBankDeprecationSchema
       Then
       -- ����� ����� ��� ������
       mk_current_dpr_bnk( pn_agrId => pn_agrId -- �������
                         , pn_part => pn_part -- ����� - ���� �� ����������...
                         , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������
                         , pb_isdbms => pb_isdbms  -- ������� ������ ���������� � dbms_output
                         , pa_result => pa_result
                         -- z.196126 10.10.2019
                         , pb_correction_required =>  pb_correction_required
                        -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                         --- Vct 15.05.2020
                         , pc_dpr_code => vc_dpr_code -- ����� ����������� ���������� ����������
                         , pb_Write_to_grpLog => bWrite_to_grpLog -- true - ��������� CDGRP.LOG_PUT(
                         );
     Else
       -- ������ ����� ��� MFO
       mk_current_dpr_inner( pn_agrId => pn_agrId -- �������
                           , pn_part => pn_part -- ����� - ���� �� ����������...
                           , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������
                           , pb_isdbms => pb_isdbms -- ������� ������ ���������� � dbms_output
                           , pa_result => pa_result
                           -- z.196126 10.10.2019
                           , pb_correction_required =>  pb_correction_required
                           --- Vct 15.05.2020
                           , pc_dpr_code => vc_dpr_code -- ����� ����������� ���������� ����������                         
                           , pb_Write_to_grpLog => bWrite_to_grpLog -- true - ��������� CDGRP.LOG_PUT(
                           -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                           );
     End If;
   Else -- ����� ������ - ������ �� �������� ��������������� �������
   If isBankDeprecationSchema
       Then
       -- ����� ��� ������ 
       mk_current_dpr_bnk_fv( pn_agrId => pn_agrId -- �������
                         , pn_part => pn_part -- ����� - ���� �� ����������...
                         , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������
                         , pb_isdbms => pb_isdbms  -- ������� ������ ���������� � dbms_output
                         , pa_result => pa_result
                         -- z.196126 10.10.2019
                         , pb_correction_required =>  pb_correction_required
                        -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                         --- Vct 15.05.2020
                         , pc_dpr_code => vc_dpr_code -- ����� ����������� ���������� ����������
                         , pb_Write_to_grpLog => bWrite_to_grpLog -- true - ��������� CDGRP.LOG_PUT(
                         );
       
     Else
       Null;
       RAISE_APPLICATION_ERROR(cd_errsupport.i_IGNORE_RESULT,'cd_dpr_current.mk_current_dpr: ������� - ����� ���, ������� 520 � ��������� ������ �� ����������.');
       /*
       -- ����� ��� MFO
       mk_current_dpr_inner_fv( pn_agrId => pn_agrId -- �������
                           , pn_part => pn_part -- ����� - ���� �� ����������...
                           , pd_evt_date => pd_evt_date -- ����, � ������� ���������� ��������
                           , pb_isdbms => pb_isdbms -- ������� ������ ���������� � dbms_output
                           , pa_result => pa_result
                           -- z.196126 10.10.2019
                           , pb_correction_required =>  pb_correction_required
                           --- Vct 15.05.2020
                           , pc_dpr_code => vc_dpr_code -- ����� ����������� ���������� ����������                         
                           , pb_Write_to_grpLog => bWrite_to_grpLog -- true - ��������� CDGRP.LOG_PUT(
                           -- , pi_result OUT Pls_integer -- ��� ������; 0 - �����, 8192 - �� ��������� ������
                           );
       */
     End If;     
   End IF;    
 End;
 ---------------------------------------------------------------------
begin
  -- Initialization
 Null;
end cd_dpr;
/
