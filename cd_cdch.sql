create or replace package cd_cdch authid definer is

  -- Author  : DOBROVOLSKY
  -- Created : 2015-04-13 15:21:52
  -- Purpose : работа с таблицами регистрации объектов, отправленных в различные Бюро Кредитных Историй
  
    
  Version  CONSTANT VARCHAR2(200 char) := '$Id. cd_cdch {7.06.03} 13.04.2015/15.04.2021 12:46 CD Vct $';
  -------------------------------------------------------------------------------------------------------
  -- Vct 15.04.2015 - выдача пакета 
  -- Vct 29.04.2015 {6.98.03} в список описателей типов отчетов добавлен 'CR_XML' -- Vct 29.04.2015 (z.152775) - добавлено для формата Credit_Registry (не знаю, какая кодировка подразумевалась для этого формата ранее)
  -- Vct 09.02.2016 {6.99.01}  -- z.160830 - поправлена has_register_history
  -- Vct 17.02.2016 {6.99.02}  -- z.161129 - поправлена has_register_history
  -- Vct 02.03.2016 {6.99.03}  -- z.161491  - поправлена has_register_history
  -- Vct 02.08.2016 {6.99.04}  -- поправлена rule_REQUEST16 с целью выброса ошибки при неудачном выполнении функции, определяющей флаг дефолта
  -- Vct 16.08.2016 {6.99.05}  -- поправлена выдача ошики - 668
  -- Vct 01.11.2016 {7.01.01}  -- ci_RULE_REQUEST2 - для отчета 800190_30 - без пересчета состояния при получении флага дефолта
  -- Vct 05.12.2016 {7.01.02} z.167590 - ранее выводилась отказная заявка
  -- Vct 15.08.2017 {7.02.01} поправлен has_registered_history для кода 8 (800190_30)
  -- Vct 28.06.2018 {7.03.01} ClearRegisteredHistory_svk z.182475 
  -- Vct 25.01.2019 {7.04.01} z.190592 - процедуры удаления юр. документа из регистрации для использования после редактирования документа на форме.
  -- Vct 11.03.2019 {7.04.02} z.191849 has_register_history
  -- Vct 02.10.2019 {7.05.01} Register_all_reports - регистрация сегмента PA
  -- Vct 28.10.2019 {7.05.02} z.196306 
  -- Vct 08.11.2019 {7.05.03} 
  -- Vct 31.12.2019 {7.05.04} z.197857 bReplaceData_flag, DelHistory_for_Replace
  -- Vct 23.06.2020 {7.06.01} Register_all_reports, z.204079
  -- Vct 18.12.2020 {7.06.02} - опубликована в интерфейсе DelAllRegisteredHistory
  -- Vct 15.04.2021 {7.06.03} z.211947, Clear_RegHistory_For_Day, Clear_RegHistory_For_Day_AT
  -------------------------------------------------------------------------------------------------------
  -- 
  -------------------------------------------------------------------------------------------------------
  -- тип для кодов имен источников хранения зарегистрированных данных, как "отправленные в БКИ" 
  SUBTYPE T_CDCH_CODENAME Is Varchar2(50);
  ----
  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договора
  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
  -- отдельный код для обеспечений пока не нужен, т.к. они регистрируются от идентификатора документа в cdh_doc
  cc_PROVISION_Alias  T_CDCH_CODENAME := 'CPROVISION'; -- для регистрации поручительств
  ----
-- процедура очистки "временных" таблиц регистрации объектов для БКИ
  Procedure ClearCDCH_TMPTBL(pvc_SEssionID In Varchar2);

-- процедура очистки "временных" таблиц регистрации объектов для БКИ с коммитом
  Procedure ClearwCommitCDCH_TMPTBL(pvc_SEssionID In Varchar2);

-- процедура очистки "временных" таблиц регистрации объектов для БКИ в автономной транзакции
  Procedure ClearAutoTranCDCH_TMPTBL(pvc_SEssionID In Varchar2);
------------------------------------------------------------------
  -- процедура регистрации отчета 
  --копирует данные из временных таблиц в соответствующие постоянные 
  Procedure Register_all_reports(pvc_sessionID In Varchar2);  
------------------------------------------------------------------
  -- функция возвращает 1, если существуют времено зарегистрированные в сесии доаане по 
  -- объектам для отчетов БКИ, 0 в противном случае
  Function session_data_exists(pvc_sessionID in Varchar2) Return Integer;  
------------------------------------------------------------------
  -- общая ф-ция предварительной регистрации данных по объектам, перевадаваемым в БКИ
  -- допустимые значени для pvcObjectType:
  --  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
  --  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договор
  --  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
  Function Register_cdch_tempobj(pvcObjectType IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                                    , nObjID In Number -- числовой ижентификатор регистрируемого объекта
                                    , pvcSessionID in Varchar2 -- идентификатор сеанса пользователя
                                    , pvcReportType in Varchar2 -- кодовое имя типа отчета
                                    , pnAStatus in Integer DEfault 0 -- Доп. статус, (1 - исключить из дальнейшего расчета)
                                    , pdEventDate in Date Default CD.Get_LSDATE -- дата, которой регистрируем отчет
                                    ) Return Pls_integer;  
----------------------------------------------------------------------
  -- ф-ция возвращает последнюю дату регистрации объекта в отчетах БКИ
  -- допустимые значения для pvcObjectType:
  --  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
  --  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договора
  --  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
  Function Get_LastRegisterDate(pvcObjectType in Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                                , pnObjID in NUMBER -- идентификатор зарегистрированного объекта
                                , TypeRep in CDCH.ccdchtype%TYPE -- тип отчета 
                                , evDATE in DATE DEFAULT CD.get_lsdate) RETURN DATE;
----------------------------------------------------------------------
  -- ф-ция возвратит 1, если объект для БКИ был зарегистрирован c точки зрения правила сравнения,
  -- определяемого параметром piruleMode
  -- значения piruleMode: 
  -- 0 - объект считается зарегистрированным, если он зарегистрирован в дне = evDATE
  -- 1 - объект считается зарегистрированным, если он был зарегистрирован датой <= evDATE
  -- 2 - объект считается зарегистрированным, если он был зарегистрирован датой <= evDATE и доп статусом = 1 (или в дне evDate)
  Function has_register_history(pvcObjectType IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                               , pnObjID IN NUMBER -- идентификатор зарегистрированного объекта
                               , TypeRep IN CDCH.ccdchtype%TYPE -- тип отчета 
                               , piruleMode In Integer  -- код правила сравнения 0,1,2 (определения того, что объект был зарегистрирован)
                               , evDATE In DATE DEFAULT CD.get_lsdate) Return Pls_integer;
----------------------------------------------------------------------
-- Vct z.182475 
-- Для встраивания в постобработчик смены статуса договора
-- если статус договороа закрыт - чистить историю отправок по договору
-- с даты закрытия
Procedure ClearRegisteredHistory_svk(pn_agrId in Number);
----------------------------------------------------------------------
-- Vct 24.01.2019 z.190592 
-- проверка наличия документа в истории зарегистированных отправок в БКИ
-- возвращает 0, если не обнаружен в истории, 1 в обратном случае.
-- вызов cd_cdch.Document_Has_BKI_History(pn_docid in Number /* идентификатор документа */ )
Function Document_Has_BKI_History(pn_docid in Number -- идентификатор документа
                                 ) return Number;
-----------------------------------------------------------------------
-- иногда требуется повторная отправка юр. документов о судебных решениях для юр лиц
-- эта процедура целиком очищает историю регистрации отправки конкретного документа
Procedure Clear_Total_Doc_BKI_Hist(
                                  pn_docid_in in Number -- идентификатор документа
                                  );
-----------------------------------------------------------------------
-- Vct 25.01.2019 для вызова с формы cdhistdc.fmb
-- иногда требуется повторная отправка юр. документов о судебных решениях для юр лиц
-- с точки зрения пользователя обеспечивается доступность документа для последующей передачи в БКИ
-- если и когда появится очередь отправляемых документов,
-- вероятно здесь же проводить формирование этой очереди
-- вызов: cd_cdch.mk_Document_BKI_Ready_cf(pn_docid_in in Number, i_result_Out OUT Number, pc_error_message_out Out Varchar2);
Procedure mk_Document_BKI_Ready_cf(pn_docid_in in Number -- идентификатор документа
                                 , i_result_Out OUT Number -- резуль
                                 , pc_error_message_out Out Varchar2
                                 );
-----------------------------------------------------------------------
-- процедура установки флага замены данных в истории регистрации
-- 0 - снять флаг, 1 - установить  
Procedure SetReplaceData_Flag(pn_Flag In Integer);
-----------------------------------------------------------------------
-- функция установки флага замены данных в истории регистрации
-- для использования в курсорах альт. печати
-- 0 - снять флаг, 1 - установить  
-- возвращает установленной значение флага в виде целого числа
-- cd_cdch.bf_setReplaceData_Flag(pn_Flag In Integer)
Function bf_setReplaceData_Flag(pn_Flag In Integer) Return Integer;
-----------------------------------------------------------------------
-- Vct 18.12.2020 - опубликована в интерфейсе
-- Vct 28.06.2018 z.182475 удаление истории регистрации всех связанных сдоговором событий  после указанной даты
Procedure DelAllRegisteredHistory(pn_agrID in Number -- идентификатор договора
                              , pd_dateStartFrom in Date
                              , pc_rpttype in Varchar2 -- тип отчета TUTDF - 
                              );
----------------------------------------------------------------------
-- Vct 18.12.2020 - опубликована в интерфейсе
-- Vct 28.06.2018 z.182475 удаление истории регистрации всех связанных сдоговором событий  после указанной даты
-- в атономной транзакции
Procedure DelAllRegisteredHistory_AT(pn_agrID in Number -- идентификатор договора
                                   , pd_dateStartFrom in Date
                                   , pc_rpttype in Varchar2 -- тип отчета TUTDF - 
                                    );                              
-------------------------------------------------------------------------
-- z.211947
-- полное удаление истории всех выгрузок, пришдшихся на конкретный день
Procedure Clear_RegHistory_For_Day(pd_onDate in date , pc_reptype In varchar2);
-------------------------------------------------------------------------
-- полное удаление истории всех выгрузок, пришдшихся на конкретный день
-- а автономной транзакции
Procedure Clear_RegHistory_For_Day_AT(pd_onDate in date , pc_reptype In varchar2, pn_result Out Number);                                    
-------------------------------------------------------------------------
end cd_cdch;
/
create or replace package body cd_cdch is

  ci_Zero constant pls_integer := 0;
  ci_False constant pls_integer := 0;
  --
  ci_SUCCESS constant Pls_integer :=0; -- успешное завершение
  ci_OTHER_ERROR constant Pls_integer := 8192;  -- прочая ошибка (неизвестной природы)
-------------------------------------------------------------------
-- 31.12.2019 Vct
-- флаг замены данных - для отчетов 
  bReplaceData_flag Boolean := False;
  

-- процедура установки флага замены данных в истории регистрации
-- 0 - снять флаг, 1 - установить  
Procedure SetReplaceData_Flag(pn_Flag In Integer)
Is
Begin
  bReplaceData_flag := Not (coalesce(pn_Flag, ci_Zero) = ci_Zero);
End;  
-------------------------------------------------------------------
-- функция установки флага замены данных в истории регистрации
-- для использования в курсорах альт. печати
-- 0 - снять флаг, 1 - установить  
-- возвращает установленной значение флага в виде целого числа
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
-- процедура очистки "временных" таблиц регистрации объектов для БКИ
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
  Delete From CDCH_CLIPAY_TMP;  -- временная таблица
  -- Vct 08.11.2019
  Delete  cdch_clipayovr_tmp;
  Delete CDCH_CLIPAY_SQLOG_TMP;

  bReplaceData_flag := False; -- Vct 31.12.2019
  
End;  

-- процедура очистки "временных" таблиц регистрации объектов для БКИ с коммитом
Procedure ClearwCommitCDCH_TMPTBL(pvc_SEssionID In Varchar2)
is
Begin
  
  ClearCDCH_TMPTBL(pvc_SEssionID);
  COMMIT WORK WRITE BATCH NOWAIT;    
End;  
-- процедура очистки "временных" таблиц регистрации объектов для БКИ в автономной транзакции
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
-- полное удаление истории всех выгрузок, пришдшихся на конкретный день
Procedure Clear_RegHistory_For_Day(pd_onDate in date , pc_reptype In varchar2)
Is
  vc_rpttype cdch.ccdchtype%Type;
Begin
  vc_rpttype := Upper(pc_reptype);
  -- кредитные договора - cdch
  Delete /*+ cluster_by_rowid(t) */ From cdch t
  Where 1 = 1 
  And t.dcdchdate = pd_onDate --Cast( pd_onDate as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- удаление истории зарегистрированных платежей (сегменты PA).
  Delete /*+ cluster_by_rowid(p) */ From cdch_clipay p Where 1 = 1 
  And p.dcdch_registration = pd_onDate --sysdate --:d_reg
  And p.ccdchtype = vc_rpttype --'TUTDF'
  And p.icdchpa_issccode = 27 -- кредиты
  ;
  -- удаление юр. документов
  Delete /*+ cluster_by_rowid(t) */  from cdch_d t  Where 1 = 1 
  And t.dcdchdate = pd_onDate -- :p1
  And t.ccdchtype = vc_rpttype --'TUTDF'
  ;
  ---  поручительства (обеспечения и залоги) cdch_czo, v_czo_kb_1
  Delete /*+ cluster_by_rowid(t) */  from cdch_czo  t
  Where 1 = 1   
  And t.ccdchtype = vc_rpttype
  And t.dcdchdate = pd_onDate
  ;
  -- заявки клиентов    - cdch_rq, V_CDMO_Z_KB
  Delete /*+ cluster_by_rowid(t) */ From cdch_rq t
  Where 1 = 1 
  And t.dcdchdate = pd_onDate
  And t.ccdchtype = vc_rpttype
  ;  
  -----------------------------------------------------------
  -- удаление данных из временных таблиц для текущего сеанса
  ------------------------------------------------------------
  ClearCDCH_TMPTBL(sys_context('USERENV', 'SESSIONID'));
  
  
End;   
-- полное удаление истории всех выгрузок, пришдшихся на конкретный день
-- а автономной транзакции
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
-- процедура полного удаления зарегистрированных объектов для варианта работы с заменой истории
Procedure DelHistory_for_Replace
Is
Begin
  -- кредитные договора - cdch
  Delete From cdch t
  Where 1 = 1 
    And (t.ncdchagrid , t.ccdchtype) In (
          Select tmp.ncdchagrid, tmp.ccdchtype
          From cdch_tmp tmp -- v_cdch_tmp tmp 
          --Where 1 = 1 AND T.cvcdchobjtype  = 'CAGREEMENT'
       );
  -------------------------------
  -- заявки клиентов    - cdch_rq, V_CDMO_Z_KB
  Delete From cdch_rq t
  Where  1 = 1 And 
  (t.ncdchzid, t.ccdchtype) In (
     Select tmp.ncdchzid, tmp.ccdchtype
     From cdch_rq_tmp tmp
  );
  --------------------
  -- юридические документы клиента -- cdch_d, v_cdh_doc_j
  Delete from cdch_d t
  Where 1 = 1 And
    (t.ncdchdid, t.ccdchtype) In (
        Select tmp.ncdchdid, tmp.ccdchtype
        From cdch_d_tmp tmp
    );

  --  -  поручительства (обеспечения) cdch_czo, v_czo_kb_1
  Delete from cdch_czo  t
  Where 1 = 1 And
     (t.ncdchiczoid, t.ccdchtype ) in (
          Select tmp.ncdchiczoid, tmp.ccdchtype
          From cdch_czo_tmp tmp
     );
     
  --   -- Vct 07.11.2019 - таблица регистрации сегментов PA
  Delete From CDCH_CLIPAY t
  Where 1 = 1 And 
  (t.ncdchpa_objid, t.ccdchtype) In (
        Select t.ncdchpa_objid, t.ccdchtype
        From cdch_clipay_tmp tmp
  );
  
End;
--------------------------------------------------------------------

-- процедура регистрации отчета 
--копирует данные из временных таблиц в соответствующие постоянные 
Procedure Register_all_reports(pvc_sessionID In Varchar2)
is
Begin
    
  IF bReplaceData_flag
    THEN
    DelHistory_for_Replace();
    bReplaceData_flag := False;
  End If;      
  ------------------------------------------------------------------------------------------------
  -- !!NB - константы в запросе по значениям должны совпадать с объявленными в спецификации пакета
  ------------------------------------------------------------------------------------------------
  /*
   пока будем использовать merge просто как insert, игнорируя update часть
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
                        -- не регистрируем договора, по которым зафиксировано переполнение сегмента PA
                        -- предполагается, что после регистрации по ним будет проводиться второй запуск
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
  -- Vct 23.06.2020 z.204079 - поддержка повторной регистрации для закрытых договоров.     
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
  -- Vct 23.06.2020 z.204079 - поддержка повторной регистрации для закрытых договоров.
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
  -- TODO Vct 31.12.2019 - нет регистрации поручительств, и уже не помню, почему...
  -- м.б. это пробел в коде...
  -------------------
  -- Vct 02.10.2019 - регистрация сегмента AP
  MERGE INTO CDCH_CLIPAY USING (
    Select 
      t.icdchpa_issccode  -- код подсистемы (по умолчанию - 27 - для кредитных договоров)
    , t.ncdchpa_objid  -- код объекта (идентификатор кредитного договора для 27 подсистемы
    , t.ccdchtype      -- тип отчета - TUTDF/CAIS/NCB-CHDTF/XML1...
    , t.icdchpa_paynum99  -- номер платежа в диапазоне 1 - 99 (поле сегмента PA), для поля 1 сегмента PA
    , t.dcdch_paydate  -- дата платежа, - поле 5 сегмента PA,
    , t.mcdch_fulldaypay  -- сумма произведенного платежа, обязательный, длина 10, положительное целое число, если превышает 9,999,999,999 то отображается всеми девятками, поле 2 сегмента PA 
    , t.mcdch_daypayact  -- сумма произведенного платежа, за исключением просроченных платежей сроком свыше 30 дней, форматируется аналогично полю 2), поле 3 сегмента PA --
    , t.ccdch_curr  -- валюта платежа обязательное, длина 3 (RUB по умолчанию), поле 4 сегмента PA
  -- здесь могут появиться поля для поддержки поля 6 сегмента PA, пока неясно, как точно здесь поступать...
  -- 6 - Объем платежа F/P F - полностью, P - не полностью
  --
    , t.mcdch_totalact12   -- Суммарный размер фактических платежей за 12 месяцев за исключением просроченных платежей сроком свыше 30 дней, длина 10, поле 7 сегмента PA     
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
-- функция возвращает 1, если существуют времено зарегистрированные в сесии доаане по 
-- объектам для отчетов БКИ, 0 в противном случае
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
-- функции добавления регистрируемых в отчете для БКИ данных во "временные" таблицы, 
----------------------------------------------------------------------
-- ф-ция возвращает истину, если тип отчета не подходит для регистрации или идентификатор регистрируемого отчета не задан
function dont_registerobj(AgrID in NUMBER, -- идентификатор регистрируемого объекта
                          TypeRep In Varchar2 -- CDCH_TMP.ccdchtype%TYPE, -- тип отчета 
                         -- ,pvc_caller in Varchar2  -- вызывающий (пока не используется) для идентификации целевой таблицы регистрации и возможной подкрутки обращения с типом отчета
                         ) Return boolean
Is
Begin
  Return ((AgrID IS NULL) OR NOT (TypeRep IN ('TUTDF','CAIS'
                                             , 'CR_XML' -- Vct 29.04.2015 (z.152775) - добавлено для формата Credit_Registry (не знаю, какая кодировка подразумевалась для этого формата ранее)
                                             ,'NCB-CHDTF','XML3', 'INFOCREDIT','XML4','XML5','XML1'
                                               
                                            )
                                  ) 
         );
End;                           
                                                    
-- Запись договора во временную таблицу CDCH_TMP - предварительная для кредитных договоров
FUNCTION Insert_CDCH_TMP(AgrID NUMBER,
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- автономная транзакция для использования в альтернативной печати:
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
-- Запись заявки во временную таблицу CDCH_RQ_TMP - предварительная регистрация для заявок
FUNCTION Insert_CDCH_RQ_TMP(pZID NUMBER, -- Уникальный номер заявки
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- автономная транзакция для использования в альтернативной печати:
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
-- Запись документа во временную таблицу CDCH_D_TMP -- предварительная регистрация юридических документов
FUNCTION Insert_CDCH_D_TMP(pDID NUMBER, -- Уникальный номер документа
                         SID CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- автономная транзакция для использования в альтернативной печати:
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
-- запись обеспечений во временную таблицу CDCH_CZO_TMP
FUNCTION Insert_CDCH_CZO_TMP(pDID in NUMBER, -- идентификатор объекта (обеспечение)
                            SID in Varchar2, --CDCH_TMP.ccdchsessionid%TYPE,
                         TypeRep in Varchar2, -- CDCH_TMP.ccdchtype%TYPE,
                         pnAStatus in Integer,
                         evDATE DATE DEFAULT CD.get_lsdate) RETURN NUMBER IS
-- автономная транзакция для использования в альтернативной печати:
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
  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договор
  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
  ----
*/
-- общая ф-ция предварительной регистрации данных по объектам, перевадаваемым в БКИ
-- допустимые значени для pvcObjectType:
--  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
--  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договор
--  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
Function Register_cdch_tempobj(pvcObjectType IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                              , nObjID In Number -- числовой ижентификатор регистрируемого объекта
                              , pvcSessionID in Varchar2 -- идентификатор сеанса пользователя
                              , pvcReportType in Varchar2 -- кодовое имя типа отчета
                              , pnAStatus in Integer Default 0 -- Доп. статус, (1 - исключить из дальнейшего расчета)
                              , pdEventDate in Date Default CD.Get_LSDATE -- дата, которой регистрируем отчет
                              ) Return Pls_integer 
IS
-- автономная транзакция для использования в альтернативной печати:
  PRAGMA AUTONOMOUS_TRANSACTION;
  vcLocalPObjType T_CDCH_CODENAME;
  iretval pls_integer := 0;
BEGIN
  vcLocalPObjType := Upper(pvcObjectType);
  CASE vcLocalPObjType
    WHEN cc_CDAGR_Alias THEN -- 'CAGREEMENT'; -- для регистрации кредитных договоров      
     iretval := Insert_CDCH_TMP(AgrID => nObjID,
                                SID => pvcSessionID,
                                TypeRep => pvcReportType,
                                pnAstatus => COALESCE(pnAStatus,0),
                                evDATE => pdEventDate
                                );
                                
    WHEN cc_CDREQUEST_Alias THEN -- 'CREQUEST'; -- для регистрации заявок на договор
      iretval := Insert_CDCH_RQ_TMP(pZID => nObjID, -- Уникальный номер заявки
                                    SID => pvcSessionID,
                                    TypeRep => pvcReportType,
                                    pnAstatus => COALESCE(pnAStatus,0),
                                    evDATE => pdEventDate);
                                    
    WHEN cc_CDJDOC_Alias THEN -- 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
      iretval := Insert_CDCH_D_TMP(pDID => nObjID, -- Уникальный номер документа
                                   SID => pvcSessionID,
                                   TypeRep =>  pvcReportType,
                                   pnAstatus => COALESCE(pnAStatus,0),
                                   evDATE => pdEventDate);
    -- обеспечения могли бы обслуживаться как cc_CDJDOC_Alias, но предпочитаем независимую ветку
    WHEN cc_PROVISION_Alias THEN
      iretval := Insert_CDCH_CZO_TMP(pDID => nObjId, SID => pvcSessionID,
                                    TypeRep => pvcReportType, pnAStatus => COALESCE(pnAStatus,0),
                                     evDATE => pdEventDate);
                                         
  ELSE 
    cd_utl2s.TxtOut('cd_cdch.rct.1: не опознано имя типа сохраняемого объекта pvcObjectType='||pvcObjectType);    
  END CASE;   

  COMMIT WORK WRITE BATCH NOWAIT;   

  Return iretval;
EXCEPTION WHEN OTHERS THEN
  Rollback;
  RETURN 0;  
End;                                        
----------------------------------------------------------------------
-- ф-ция возвращает последнюю дату регистрации объекта в отчетах БКИ
-- допустимые значения для pvcObjectType:
--  cc_CDAGR_Alias T_CDCH_CODENAME := 'CAGREEMENT'; -- для регистрации кредитных договоров
--  cc_CDREQUEST_Alias T_CDCH_CODENAME := 'CREQUEST'; -- для регистрации заявок на договора
--  cc_CDJDOC_Alias T_CDCH_CODENAME := 'CJDOCUMENT'; -- для регистрации юридических документов (бакнкротсва и т.п.)
--  cc_PROVISION_Alias T_CDCH_CODENAME := 'CPROVISION'; -- для регистрации поручительств
Function Get_LastRegisterDate(pvcObjectType in Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                              , pnObjID in NUMBER -- идентификатор зарегистрированного объекта
                              , TypeRep in CDCH.ccdchtype%TYPE -- тип отчета 
                              , evDATE in DATE DEFAULT CD.get_lsdate) RETURN DATE 
IS
  --dRetval DATE;
  -- для зарегистрированных кредитных договоров
  Function get_cdagr_lastregdate(
                                 pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
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
  -- для зарегистрированных заявок на договора
  Function get_cdreq_lastregdate(
                                 pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
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
  -- для зарегистрированных юр. документов
  Function get_jdoc_lastregdate(
                                 pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
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
 -- обеспечения могли бы обслуживаться как юр. документы, но предпочитаем собственную ветку
  -- для зарегистрированных обеспечений                           
  Function get_czo_lastregdate(
                                 pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                               , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
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
-- ф-ция возвратит 1, если объект для БКИ был зарегистрирован c точки зрения правила сравнения,
-- определяемого параметром piruleMode
-- значения piruleMode: 
-- 0 - объект считается зарегистрированным, если он зарегистрирован в дне = evDATE
-- 1 - объект считается зарегистрированным, если он был зарегистрирован датой <= evDATE
-- 2 - объект считается зарегистрированным, если он был зарегистрирован датой <= evDATE и доп статусом = 1 (или в дне evDate)
Function has_register_history(pvcObjectType IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                             , pnObjID IN NUMBER -- идентификатор зарегистрированного объекта
                             , TypeRep IN CDCH.ccdchtype%TYPE -- тип отчета 
                             , piruleMode In Integer  -- код правила сравнения 0,1,2 (определения того, что объект был зарегистрирован)
                             , evDATE In DATE DEFAULT CD.get_lsdate) Return Pls_integer
is

  ci_Zero constant Pls_Integer := 0;
  ci_ONE constant Pls_integer := 1;
  ci_Two constant Pls_Integer := 2;
  ci_RULE_REQUEST constant Pls_Integer := 4; -- при этом значении вызов функции получения флага дефолта по договору 
                                    -- произойдет с пересчетом состояния по договору (и регистрацией во временной таблице) 
  ------------------------------------------------------------------------------------------------------------------------
  ci_RULE_REQUEST2 constant Pls_Integer := 8; -- при этом значении вызов функции получения флага дефолта по договору 
                                     -- произойдет с пересчетом состояния по договору (и регистрацией во временной таблице) 
  -------------------------------------------------------------------------------------------------------------------------
  ci_noRecalcState constant Pls_integer := 0;
  ci_RecalcState constant Pls_integer := 1;
  --------------------------------------                                    
  --
  ilocalAttr Pls_integer;

  -- правило 0 - день регистрации совпадает с поданным на вход.
  Function rule_zero(pvcObjectType2 IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                    , pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
                    , evDATE2 In DATE) Return Pls_integer
  is
   dateRg date;                                 
  Begin
     dateRg := Get_LastRegisterDate(pvcObjectType2, pnObjID2, TypeRep2, evDATE2);                              
     RETURN CASE cd_utl2s.is_equal(dateRg,evDate2) WHEN True Then ci_One Else ci_Zero End;
  End;                    
  -- правило 1 - день регистрации <= evDate
  Function rule_one(pvcObjectType2 IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                    , pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
                    , evDATE2 In DATE) Return Pls_integer
  is
   dateRg date;                                 
  Begin
     dateRg := Get_LastRegisterDate(pvcObjectType2, pnObjID2, TypeRep2, evDATE2);                              
     RETURN CASE dateRg < evDate2 OR cd_utl2s.is_equal(dateRg,evDate2) WHEN True Then ci_One Else ci_Zero End;
  End;      
  -- правило 2 - есть запись с доп. статусом 1 в предшествующем дне
  Function rule_two(pvcObjectType2 IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                    , pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                    , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
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
      Null;  -- вариант не опознан, но ошибку подавили
    END CASE;  
    return nretval;
  end;  

  -- Vct 09.02.2016 -- пока только для заявок на кредиты (сегмент IP в TUTDF)
  -- z.160830
  Function rule_REQUEST16(
                          pvcObjectType2 IN Varchar2 -- тип регистрируемого объекта, из списка констант ..._Alias
                        , pnObjID2 IN NUMBER -- идентификатор зарегистрированного объекта
                        , TypeRep2 IN CDCH.ccdchtype%TYPE -- тип отчета 
                        , evDATE2 In DATE 
                        , pi_recalc_state in Pls_integer    
                         ) Return Pls_integer 
  Is
    SUBTYPE T_AGR_DEFAULTFLAG is Varchar2(2); -- тип для флага "дефолта" по договору

    SUBTYPE T_AGR_STATUS is "cda".ICDASTATUS%Type; -- статус договора
    SUBTYPE T_AGR_NUMBER is  "cda".NCDAAGRID%Type; -- номер договора

    ci_AGRCLOSED_STATUS constant T_AGR_STATUS := 3; -- кредитный договор закрыт
    ci_AGRREFUSED_STATUS constant T_AGR_STATUS := 4; -- отказано -!!! это ошибка!!, 4 - это договор в статусе заявка! 
    ci_AGRDRAFT_STATUS constant T_AGR_STATUS := 0; -- черновик
    ci_AGRPRELIMINARY_STATUS constant T_AGR_STATUS := 1; -- предварительный (условный)
    ci_AGRWORKING_STATUS constant T_AGR_STATUS := 2; -- работающий договор
    
    nretval Pls_integer := ci_Zero;
    vcObjectType_loc T_CDCH_CODENAME;  
    iApproved_lc Integer; -- была регистрация объекта в состоянии заявка одобрена
    iClosed_lc Integer; -- была регистрация объекта в состоянии договора закрыт
    iToday_lc Integer;  -- была регистрация объекта сегодня
    -- Vct 11.03.2019 z.191849
    iRefused_lc Integer; -- была регистрация отказной заявки
    
    -- Vct 05.12.2016 z.167590 - отказные заявки не выгружать повторно
    iZeroStatus_lc Integer; -- заявка выгружалась со статусом 0
    dlast_registered Date;  -- дата последней регистрации заявки
    dRefuseDate_lc Date; -- дата отказа по заявке
    ----------------
    iAgrStatus_lc T_AGR_STATUS; -- статус договора - (3 - закрыт)
    iAPPRVAL_lc Integer; -- статус одобрения заявки
    
    dcdaclosedDate_lc Date; -- дата закрытия договора
    vnAgrID T_AGR_NUMBER; -- номер кредитного договора
    
    -- TODO !!!!- обойтись с ошибкой расчета состояния!!!!!
    vc_default120 T_AGR_DEFAULTFLAG; --cd_types.TVCHAR2000; --T_AGR_DEFAULTFLAG;
    vc_temp_default_flag cd_types.TErrorString;
    
  Begin
    vcObjectType_loc := upper(pvcObjectType2);
    CASE vcObjectType_loc
      WHEN cc_CDREQUEST_Alias THEN
        -- получаем статистику регистрации заявки
        -----------------------------------------------------------------------------
        -- заявку выводить один раз, если она одобрена,
        -- один раз, когда договор закрыт и всегда, пока договор в состояни дефолта.
        -- на текущий момент здеь состояние дефолта не анализируем - анализ дефолта в использующем запросе
        -----------------------------------------------------------------------------
      Select  /*+ cd_cdch.rule_REQUEST16.1 */
         --  COUNT(*) as ntotal, -- всего регистрационных записей
          SUM(CASE WHEN T.ivcdchastatus = 1 then 1 END) as napproved -- выгружалось записей в статусе "заявка ободрена"
        , SUM(CASE WHEN T.ivcdchastatus = 3 then 1 END) as nClosed   -- выгружалось записей в состоянии договор закрыт
        , SUM(CASE WHEN trunc(T.dvcdchrepdate) = evDATE2 THEN 1 END) ntoday -- заявка выгружалось сегодня
        , SUM(CASE WHEN T.ivcdchastatus = 0 then 1 END) as nZeroStatus   -- выгружалось со статусом 0
        -- Vct 11.03.2019
        , SUM(CASE WHEN T.ivcdchastatus = 2 then 1 END) as nRefusedStatus   -- выгружалось со статусом 2 (отказная)
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
       -- получим состояние договора по заявке
      IF COALESCE(iToday_lc,0) = 0 
        THEN -- заявка сегодня не выводилась в отчет, смотрим ее статус
        Begin 
          Select /*+ cd_cdch.rule_REQUEST16.2 */
                 COALESCE(cda.ICDASTATUS,0), COALESCE(t_kb.APPRVAL,0)
          ,cda.NCDAAGRID, cda.DCDACLOSED -- Vct 02/08/2016  
          , Trunc(t_kb.DZDATERFSD) -- дата отказа (указывается со временем, поэтому trunc, Vct 11.03.2019)
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
              RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR,'cd_cdch.656:Ошибка доступа к кредиту по заявке-<'
                                    ||pnObjID2||'>:'||cd_errsupport.format_ora_errorstack(true));              
        End;
        vc_default120 := 'Z';
        -- Vct 17/02/2016 z.161129
        IF  iAPPRVAL_lc = 1 -- заявка одобрена
            AND (iAgrStatus_lc = ci_AGRWORKING_STATUS
                 OR (iAgrStatus_lc = ci_AGRCLOSED_STATUS And dcdaclosedDate_lc > evDATE2)
                 ) -- Vct для закрытого договора будем смотреть, если текущая дата меньше даты закрытия договора
                     --    NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- договор не закрыт
            AND iAPPRVAL_lc = 1 -- заявка одобрена
          THEN -- договор не закрыт - смотрим состояние дефолта    
          vc_temp_default_flag := COALESCE(cdrep_util3.get_agrdefault_flag(pn_agrid => vnAgrID, pd_ondate => evDATE2,pi_recalc_flag => pi_recalc_state),'Z');
          IF vc_temp_default_flag NOT IN ('Z','D')
            THEN
              cd_utl2s.TxtOut('cd_cdch.668:Ошибка при определении флага дефолта. заявка-<'
                                     ||pnObjID2||'>, договор - <'||vnAgrID
                                     -- Vct 16.08.2016 добавлен показ самого флага
                                     ||'>: pd_date ='||to_char(evDATE2, 'DD.MM.YYYY')
                                     ||' pi_recalc_state='||pi_recalc_state -- Vct 15.08.2017
                                     ||' vc_temp_default_flag='||vc_temp_default_flag);
                                     
              RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR,'cd_cdch.668:Ошибка при определении флага дефолта. заявка-<'
                                     ||pnObjID2||'>, договор - <'||vnAgrID
                                     -- Vct 16.08.2016 добавлен показ самого флага
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
                    WHEN iToday_lc > 0 THEN 1 -- заявка сегодня уже выводилась
                    WHEN iAgrStatus_lc IN ( ci_AGRCLOSED_STATUS --, ci_AGRREFUSED_STATUS -- rem Vct 11.03.2019
                                          )
                         AND ((iToday_lc > 0) OR (iClosed_lc > 0))
                        THEN 1 -- ранее выводилась заявка в состоянии договор закрыт
                            
                    WHEN 1 = 1 -- And iAgrStatus_lc NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- rem Vct 02.08.2016
                        AND iAPPRVAL_lc = 1 
                        AND ((iToday_lc > 0) OR iApproved_lc > 0 )
                        AND vc_default120 = 'Z' -- нет состояния дефолта
                        THEN 1 -- ранее выводилась заявка в состоянии одобрена 
                    WHEN vc_default120 = 'D' -- есть состояния дефолта
                         -- AND iAgrStatus_lc NOT IN ( ci_AGRCLOSED_STATUS , ci_AGRREFUSED_STATUS) -- rem Vct 02.08.2016
                         AND iToday_lc > 0 
                         THEN 1 -- договор по заявке в состоянии дефолта и заявка сегодня уже выводилась
                    -- Vct 05.12.2015 z.167590 - ранее выводилась отказная заявка
                    --WHEN dRefuseDate_lc <= evDATE2 AND (iZeroStatus_lc > 0 OR iToday_lc> 0)
                    WHEN dRefuseDate_lc <= evDATE2 AND (iRefused_lc > 0 OR iToday_lc> 0)                      
                      THEN 1
                  ELSE 0  -- считаем, что заявка должна попасть в отчет
                  END;    
       --------------  
       Return nRetval;              
    ELSE
      -- здесь выброс ошибки, поэтому нет возврата значения...
      RAISE_APPLICATION_ERROR(cd_errsupport.i_COMMON_ERROR, 'cd_cdch.592:ERROR: для объектов типа <'
                            ||vcObjectType_loc||'> вызов функции с вариантом алгоритма <'||piruleMode
                            ||'> не предусмотрен. Сообщите в службу поддержки.'
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
-- Vct 28.06.2018 z.182475 удаление истории регистрации всех связанных сдоговором событий  после указанной даты
Procedure DelAllRegisteredHistory(pn_agrID in Number -- идентификатор договора
                              , pd_dateStartFrom in Date
                              , pc_rpttype in Varchar2 -- тип отчета TUTDF - 
                              )
is
 vc_rpttype cdch.ccdchtype%Type;
Begin
  vc_rpttype := Upper(pc_rpttype);
  -- кредитные договора - cdch
  Delete From cdch t
  Where t.ncdchagrid = pn_agrID
  And t.dcdchdate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- заявки клиентов    - cdch_rq, V_CDMO_Z_KB
  Delete From cdch_rq t
  Where 
  t.ncdchzid in (Select tt.IZID
                 From v_cdmo_z_kb tt where tt.NZAGRID = pn_agrID
                 )
  And t.dcdchdate >= pd_dateStartFrom -- Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  -- юридические документы клиента -- cdch_d, v_cdh_doc_j
  Delete from cdch_d t
  Where 
    t.ncdchdid in (Select tt.ICDHID
                   From v_cdh_doc_j tt
                   Where tt.NCDHAGRID = pn_agrID
                  ) 
  And t.dcdchdate  >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  And t.ccdchtype = vc_rpttype
  ;
  --  -  поручительства (обеспечения) cdch_czo, v_czo_kb_1
  Delete from cdch_czo  t
  Where 
  t.ncdchiczoid in (select tt.ICZO
                    from v_czo_kb_1 tt
                    where tt.NCZOAGRID = pn_agrID --= 2104 --:pp
                   )
  And t.ccdchtype = vc_rpttype
  And t.dcdchdate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  ;
  --   -- Vct 07.11.2019 - таблица регистрации сегментов PA
  Delete From CDCH_CLIPAY t
  Where t.ccdchtype = vc_rpttype
  And t.ncdchpa_objid =  pn_agrID
  And t.icdchpa_issccode = 27 -- кредиты
  And t.dcdch_paydate >= pd_dateStartFrom --Cast( pd_dateStartFrom as date)
  ;  
  -----
End;  
----------------------------------------------------------------------
----------------------------------------------------------------------
-- Vct 28.06.2018 z.182475 удаление истории регистрации всех связанных сдоговором событий  после указанной даты
-- в атономной транзакции
Procedure DelAllRegisteredHistory_AT(pn_agrID in Number -- идентификатор договора
                                   , pd_dateStartFrom in Date
                                   , pc_rpttype in Varchar2 -- тип отчета TUTDF - 
                                    )
is
  Pragma autonomous_transaction;
Begin
  ----------------------------------------------------------------------
  Begin
    DelAllRegisteredHistory(pn_agrID => pn_agrID -- идентификатор договора
                              , pd_dateStartFrom => pd_dateStartFrom
                              , pc_rpttype => pc_rpttype -- тип отчета TUTDF - 
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
-- Для встраивания в постобработчик смены статуса договора
-- если статус договороа закрыт - чистить историю отправок по договору
-- с даты закрытия
-- вызов cd_cdch.ClearRegisteredHistory_svk(pn_agrId in Number)
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
      DelAllRegisteredHistory_AT(pn_agrID => pn_agrId -- идентификатор договора
                               , pd_dateStartFrom => vd_dcdaclosed
                               , pc_rpttype => cc_rpttype -- тип отчета TUTDF - 
                                );      
  END IF;  
End;    
----------------------------------------------------------------------
-- Vct 24.01.2019 z.190592 
-- проверка наличия документа в истории зарегистированных отправок в БКИ
-- возвращает 0, если не обнаружен в истории, 1 в обратном случае.
Function Document_Has_BKI_History(pn_docid in Number -- идентификатор документа
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
      And rownum = 1  -- на случай множественных записей в истории
      ;
  Exception
      When no_data_found Then
        -- документ не найден в истории, возвращаем 0
        nret_val := ci_False;
  End;  
  Return nret_val;
End;  
----------------------------------------------------------------------
-- иногда требуется повторная отправка юр. документов о судебных решениях для юр лиц
-- эта процедура целиком очищает историю регистрации отправки конкретного документа
Procedure Clear_Total_Doc_BKI_Hist(
                                  pn_docid_in in Number -- идентификатор документа
                                  )
is
Begin
  Delete from cdch_d t
  Where 
    t.ncdchdid = pn_docid_in
  ;
End;

----------------------------------------------------------------------
-- Vct 25.01.2019 для вызова с формы cdhistdc.fmb
-- иногда требуется повторная отправка юр. документов о судебных решениях для юр лиц
-- с точки зрения пользователя обеспечивается доступность документа для последующей передачи в БКИ
-- если и когда появится очередь отправляемых документов,
-- вероятно здесь же проводить формирование этой очереди
Procedure mk_Document_BKI_Ready_cf(pn_docid_in in Number -- идентификатор документа
                                 , i_result_Out OUT Number -- резуль
                                 , pc_error_message_out Out Varchar2
                                 )
is
-- TODO - решить вопрос о том, должен ли этот код выполняться в автономной транзакции
-- (на момент 25.01.2019 в форме cdhistdc.fmb общий коммит на OK, закрывающий форму, пока следуем логике отложенного коммита)
Begin
  Begin
    Clear_Total_Doc_BKI_Hist(
                            pn_docid_in => pn_docid_in -- идентификатор документа
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
