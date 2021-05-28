create or replace package cd_dpr authid Definer is

  -- Author  : VictorD
  -- Created : 2018-04-10 15:43:16
  -- Purpose : заведен для выноса сюда расчтеных процедур амортизации, используемых в групповых процессах или непосредственно заполняющих cde.

  -- Vct 10.04.2018 - старт пакета
  -- Vct 22.06.2018 - исправлено формирование 409/410 действий
  -- Vct 11.10.2018 {7.03.03}
  -- Vct 15.10.2018 {7.03.04} z. 1184692
  -- Vct 26.10.2018 {7.03.05} опубликован в интерфейск пакета Print_Potok
  -- Vct 12.11.2018 {7.04.01}
  -- Vct 22.11.2018 {7.04.02} z.186064
  -- Vct 23.11.2018 {7.04.02} z.186064
  -- Vct 05.12.2018 {7.04.03} z.185823
  -- Vct 25.12.2018 {7.04.04} z.186861 поправлена текущая амортизация, открыт код непроцентной корректировки
  -- Vct 29.12.2018 {7.04.05} mk_current_dpr, добавлен PUT_LOG для случая запуска в групповом процессе
  -- Vct 01.02.2019 {7.04.06} mk_current_dpr, z.190837
  -- Vct 13.02.2019 {7.04.07} Put_Array_CDD, mk_initial_dpr z.190663
  -- Vct 14.02.2019 {7.04.08} замена mk_current_dpr для работы  по частям.
  -- Vct 19/02/2019 {7.04.09} поправки в mk_initial_dpr a_potok_xnpv.First --i_xnpv_pos --
  -- Vct 26.02.2019 {7.04.10} mk_current_dpr, z.191488 замена способа вычисления непроцентной корректировки при проведении текущей амортизации
  -- Vct 27.02.2019 {7.04.11} mk_initial_dpr событие 409/410 при проведении начальной амортизации метим нулевым подтипом.
  -- Vct 05.03.2019 {7.04.12} mk_current_dpr,
  -- Vct 06.03.2019 {7.04.13} mk_current_dpr, z.191811
  -- Vct 10.03.2019 {7.04.14} mk_current_dpr,  z.192649
  -- Vct 06.06.2019 {7.05.01} mk_current_dpr_inner,mk_current_dpr z.193496
  -- Vct 15.07.2019 {7.05.02} mk_current_dpr_inner,mk_current_dpr z.194473
  -- Vct 19.07.2019 {7.05.03} Find_initial_Parts,   z.194593
  -- Vct 19.07.2019 {7.05.04}  z.194702, исправлен mk_current_dpr_bnk
  -- Vct 28.08.2019 (7.05.05) z.195326
  -- Vct 05.09.2019 (7.05.06) z.195326
  -- Vct 09.09.2019 (7.05.07) исправлено использование cd_psk_sutl.mk_datepoint_inflow
  -- Vct 03.10.2019 {7.05.08} z.195805 исправления в mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 06.10.2019 {7.05.09} z.195805 исправления в mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 09.10.2019 {7.05.10} Get_CliPay_ByPart + ...
  -- Vct 11.10.2019 {7.05.11} z.196254
  -- Vct 17.10.2019 {7.05.12} ... борьба с непроцентной корректировкой продолжается... 17:39
  -- Vct 31.10.2019 {7.05.13} z.196758, mk_current_dpr_inner, mk_current_dpr_bnk
  -- Vct 28.02.2020 {7.05.14} z.201209
  -- Vct 17.03.2020 {7.05.15} z.202252, 202298 - перерасчёт состояния без автономной транзакции 
  -- Vct 15.04.2020 {7.05.16} z.203025
  -- Vct 11.12.2020 {7.06.01} z.208543 
  -- Vct 08.04.2011 {7.06.02}  mk_current_dpr_bnk_fv
  ---------------------------------------
  -- 
  ---------------------------------------
  -- Public constant declarations
  Version CONSTANT VARCHAR2 (250) := ' $Id: {cd_dpr} {7.06.02} {2018.04.10/2021.04.08} Vct 17:21 $';

  -- record для накопления параметров вызова регистрации события в cde
  TYPE T_CDE_REGEVENT_CALLPRM_RT is Record(
      ncdeAgrid cde.ncdeagrid%Type      -- договор
    , icdePart cde.icdepart%Type        -- часть
    , icdeType cde.icdepart%Type        -- тип события
    , icdeSubType  cde.icdesubtype%Type -- подтип события
    , dcdeDate cde.dcdedate%Type -- дата обытия
    , mcdeSum cde.mcdesum%Type  -- сумма
    , ccdeRem cde.ccderem%Type  -- комментарий к операции
    , ncdeCZO CDE.ncdeCZO%TYPE
    -- Vct 15.10.2018 - признак - формировать декларативное действие или нет
    , cDeclarative CDD.CCDDNOTRN%TYPE -- Varchar2(2) -- Y - декларативно, Null - с проводкой
    , cCURISO cda.CCDACURISO%Type -- Vct 13.02.2019 - валюта для сумм
--  TODO (новое поле)  , ncd4prority   CDD.icddprior_cd4%Type -- прямой приоритет операции в cd4 для выбора на этапе генерации (приоритет выбора проводки в cd4)
  );
  --------------------------------------------------------------------------------
  -- массив параметров вызовов процедуры регистрации событий
  -- для заполнения cde в процедурах выведения стартовой истории амортизации
  -- и процедуры формирования амортизационных записей в периоде.
  Type T_CDE_CALL_QUEUE_PLTAB is table of T_CDE_REGEVENT_CALLPRM_RT Index by Pls_integer;
 -------
 isDBMS Boolean := True;
 -------------------------------------------------------------------
 -- процедура для использования  в cdgrp.Recalc_CDD_Item302
 -- для реализации группового процесса '1051' - текущая амортизация
 -- специального кода для частей пока нет - по состоянию на 10.04.2018
 Procedure mk_current_dpr( pn_agrId in Number -- договор
                         , pn_part in Integer -- часть - пока не используем...
                         , pd_evt_date in Date -- дата, в которой проводится операция
                         , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + nocopy
                         --- z.196126 Vct 10.10.2019
                         -- параметр обязательного выведения корректировки остатка сводного счета процентных/непроцентных корректировок
                         , pb_correction_required In Boolean Default False
                         ---
                        -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                         );
  -------------------------------------------------------------------
  -- отладочная печать потока в dbms_output
  -- опубликована в интерфейсе 26.10.2018
  Procedure Print_Potok(pc_prefix in Varchar2
                       , a_potok_xnpv in cd_types.t_pltab_potok2
                       , pn_isdbms In Integer --Default 0 -- 1 - формировать dbms_output, 0 - не выводить dbms_output
                       );
  -------------------------------------------------------------------
  -- Vct 22.11.2018 z.186064
  -- получение значения настройки (из фпз) указывающей, с какой стадии начинать применять коэффициент резервирования
  -- для вычисления базы амортизационного дохода
  Function get_ac_discount_from_stage(pd_onDate in date) return Number;

  -------------------------------------------------------------------
  -- Vct 13.02.2019
  -- сохранение массива рассчитанных действий в cdd
 -- Vct 31.10.2019 - убрал модификатор OUT для параметра a_result - сам массив в процедуре по факту не меняется...
  Procedure Put_Array_CDD(a_result    IN /* OUT */ cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                        , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "приоритет", считаем, что приедет при вызове из cdgrp
                        , CDACURISO   CDA.CCDACURISO%TYPE -- валюта договора, ожидаем, что приедет  при вызове из cdgrp
                        , pb_isdbms in Boolean -- признак вывода информации в dbms_output -- Vct 27.02.2019
                         );
  -------------------------------------------------------------------
  -- Vct 13.02.2019
  -- TODO - написать подобную процедуру длявызова из точки входа
  -- она должна сама перебирать части и работать только с нужной. А по завершении очищать кеш
  ---------------
  -- начальная амортизация для вызова из cdgrp.Recalc_CDD_Item302 для TypeMask in ('401','402')
  -- (замена прямого вызова cd_psk.get_XNPV в cdgrp)
  Procedure mk_initial_dpr( pn_agrId in Number -- договор
                          , pn_part in Integer -- часть - пока не используем...
                          , pd_evt_date in Date -- дата, в которой проводится операция
                          , pn_calcstate In Number -- 1 - проводить расчет состояния в процедуре, 0 - нет
                          , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                          , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "приоритет", считаем, что приедет при вызове из cdgrp
                          , CDACURISO   CDA.CCDACURISO%TYPE -- валюта договора, ожидаем, что приедет  при вызове из cdgrp
                          );
 -------------------------------------------------------------------
  -- очистка глобальной переменной   gr_initialdpr_parts
 Procedure Clear_initialdpr_parts;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
 -- Vct 04.06.2019
 -- функция возвращает True, если включена настройка ведения амортизации для банков,
 -- то есть на одной паре счетов вместе с начальной амортизацией
 -- False подразумевает схему для микрофинансов - амортизация на двух парах счетов.
 Function isBankDeprecationSchema Return Boolean;
 -------------------------------------------------------------------
 -- Vct 14.10.2019 z.194874 получение текущей "балансовой стоимости" договора
 -- = остаток на ссудном счете плюс непогашенные требования по %% и комиссиям
 -- для использования в проверочном отчете 800707_152
 function get_balance_sheet_value(pn_agrID In Number -- идентификатор договора
                                , pd_onDate in Date -- дата, на которую должны быть получены остатки
                                  ) Return Number;
 -------------------------------------------------------------------
end cd_dpr;
/
create or replace package body cd_dpr is



  -- коды возвращаемых ошибок
--  ci_SUCCESS constant Pls_integer :=0; -- успешное завершение
--  ci_NULLVALUE constant Pls_integer :=1; -- расчитанное значение ставки пусто
--  ci_UNKNOWN_ERROR constant Pls_integer := 8192;  -- прочая ошибка (неизвестной природы)
---------------------
 ci_One constant Pls_integer := 1;
 ci_Zero constant Pls_integer := 0;
 cn_Zero constant Number := 0.0;
-- ci_True constant pls_integer := 1;
 --
 cn_100 constant Number := 100.0;
 ci_One_Hundredth constant Number := 0.01;
 ------------------------------------
 -- признак, что что значение параметра модуля выставлено в 1
 cc_ParamValSetted constant cd0.ccd0value%Type := '1';
 ------------------------------------
 cn_CDBAL_ACC_AFTER constant Number := 520; -- счет амортизации после потока в cdbal
 --cn_CDBAL_ACC_REGISTRY_INCOME constant Number := 521; -- счет учетного дохода по амортизации
 ----
 ci_ISDBMS constant Pls_integer := 1;
-------------------------------------------------------------------
-- Vct 15.10.2018
  cc_Declarative_event constant CDD.CCDDNOTRN%TYPE := 'Y';
 ------------------------------------------------------------------
/*
    -- , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                           -- , pa_parts_out OUT cd_types.T_PLDICT_NUMBER -- массив найденных частей
                            )
*/
 
  -- Vct 15.04.202 - пока решил начальную и текущую амортизацию обслуживать собственными глобальными переменными.
  -- тип для текущей амортизации
  TYPE T_REC_CURRENT_DPR_AGR Is Record(
     fn_agrID Number -- договор
   , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
   , fd_evt_date Date -- дата, в которой проводится операция   
   , fb_recalc_state Boolean -- истина - требуется провести пересчет состояния по договору
  );
  
 
  -- тип для начальной амортизации
  -- тип для глобальной переменной, хранит договор, способ ведения аморьтизации по договору и набор частей, которые следует обрабатывать
  TYPE T_REC_INITIAL_DPR_AGR_CND Is Record(
      fn_agrID Number -- договор
    , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
    , a_parts cd_types.T_PLDICT_NUMBER -- массив частей, для которых проводить амортизацию, индекс массива равен номеру части
  );

  -- используется для начальной амортизации
  gr_initialdpr_parts T_REC_INITIAL_DPR_AGR_CND;

  -- Vct 15.05.2020
  -- для текущей амортизации
  gr_currentdpr T_REC_CURRENT_DPR_AGR;
  
 ------ служебные процедуры ---------------------------------------
 -- Vct 22.12.2018  принудительное подавление пакетной переменной isDBMS при работе в "режиме job-а"
  Procedure Check_DBMSOUT_Job_Mode
    Is
  Begin
    -- если истина - подавляем вывод в dbms_output
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
 -- очистка кеша для текущей амортизации
  Procedure Clear_currentdpr_cache
  Is
  -- для текущей амортизации
  vr_currentdpr T_REC_CURRENT_DPR_AGR;
  Begin
    vr_currentdpr.fb_recalc_state := True;
    gr_currentdpr := vr_currentdpr;
  End;    
 -------------------------------------------
 -- очистка глобальной переменной   gr_initialdpr_parts
 Procedure Clear_initialdpr_parts
   is
   vr_initialdpr_parts T_REC_INITIAL_DPR_AGR_CND; -- структура-пустышка для очистки глобальной переменной

 Begin
   -- так как вызывается в cdgrp перед обработкой договора,
   -- совместим с проверкой режима работы в автономном джобе и для такого случая отключим
   Check_DBMSOUT_Job_Mode();
   ---
   gr_initialdpr_parts := vr_initialdpr_parts;
   
   -- Vct 15.05.2020
   -- в cdgrp есть вызов Clear_initialdpr_parts
   -- пока совмещаем очистку глобальной переменной
   -- для текущей амортизации
   -- TODO - может быть потом всё-таки совместить работу начальной и текущей амортизации в одной пересенной
   Clear_currentdpr_cache();
   --
 End;
 ------------------------------------------------------------------
 -- установка глобальной переменной для текущей амортизации
 Procedure Setup_currentdpr_gchache(
                                   pn_agrID In Number -- договор
                                 , pc_dprcode In cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                                 , pd_evt_date In Date -- дата, в которой проводится операция   
                                  )
   Is
   /*
  -- тип для текущей амортизации
  TYPE T_REC_CURRENT_DPR_AGR Is Record(
     fn_agrID Number -- договор
   , fc_dprcode cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
   , fd_evt_date Date -- дата, в которой проводится операция   
   , fb_recalc_state Boolean -- истина - требуется провести пересчет состояния по договору
  );   
   */
   vr_currentdpr T_REC_CURRENT_DPR_AGR;
   Procedure mk_new_dbp_cache(
                             pn_agrID In Number -- договор
                           , pc_dprcode In cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                           , pd_evt_date In Date -- дата, в которой проводится операция   
                           , pr_newdpr OUT NOCOPY T_REC_CURRENT_DPR_AGR                           
                            )
   Is   
   Begin
     pr_newdpr.fn_agrID := pn_agrID;
     pr_newdpr.fc_dprcode := pc_dprcode;
     pr_newdpr.fd_evt_date := pd_evt_date;
     pr_newdpr.fb_recalc_state := True; -- пересчитывать состояние     
   End;  
 Begin
   vr_currentdpr := gr_currentdpr;

   IF cd_utl2s.is_equal(vr_currentdpr.fn_agrID, pn_agrID) 
     THEN -- тот же самый договор
     -- переустановка флага пересчёта состояния         
     vr_currentdpr.fb_recalc_state := Case cd_utl2s.is_equal(vr_currentdpr.fd_evt_date, pd_evt_date)
                                      When True THEN
                                        -- совпал договор и дата расчета - снимаем флаг пересчета состояния
                                        False
                                      Else
                                        -- устанавливаем флаг пересчета состояния
                                        True
                                     End;       
   ELSE
     -- новый договор
     mk_new_dbp_cache(
                     pn_agrID => pn_agrID -- договор
                   , pc_dprcode => pc_dprcode -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                   , pd_evt_date => pd_evt_date -- дата, в которой проводится операция   
                   , pr_newdpr => vr_currentdpr
                    );
   END IF;    
   gr_currentdpr := vr_currentdpr;
 End;     
 ------------------------------------------------------------------
 -- округление в деньги -------------------------------------------
  Function to_money(pn_sum in Number) return Number
  is
  Begin
    Return Round(pn_sum,2);
  End;
  ---------------------------------------------------
  -- форматирование даты для выдачи в dbms_output
  Function fmt_date_out(pd in Date) Return Varchar2
  Is
  Begin
    Return to_char(pd,'DD.MM.YYYY');
  End;
  ---------------------------------------------------
  -- выдача в dbms-output
  Procedure db_out(pc_prefix in Varchar2, pc_text in Varchar2)
    Is
  Begin
    IF isDBMS THEN
      cd_utl2s.TxtOut((pc_prefix||':')||pc_text);
    END IF;
  End;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
 -- Vct 14.10.2019 z.194874 получение текущей "балансовой стоимости" договора
 -- = остаток на ссудном счете плюс непогашенные требования по %% и комиссиям
 -- для использования в проверочном отчете 800707_152
 -- вызов cd_dpr.get_balance_sheet_value(pn_agrID In Number, pd_onDate in Date)
 function get_balance_sheet_value(pn_agrID In Number -- идентификатор договора
                                , pd_onDate in Date -- дата, на которую должны быть получены остатки
                                  ) Return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- ссудная задолженность
   n_retval := Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                       , defdate_in => pd_onDate
                                       ,TYPEACC1_in => 1   -- остаток ссудной задолженности для обычного договора
                                       ,TYPEACC2_in => 701 -- остаток ссудной задолженности для цессии
                                       ), cn_Zero);
   -----
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 10  -- счет требований банка
                                                 -- ,TYPEACC2_in => 101
                                                  ,TYPEACC2_in => 40 -- счет учета накопленной комиссии
                                                  ), cn_Zero);
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 705 -- счет приобретенных прав %%
                                                  ,TYPEACC2_in => 781 -- счет приобретенных прав комиссий
                                                  ),cn_Zero);
   Return n_retval;
 End;
 -------------------------------------------------------------------
 -- счета требований банка
 function get_accrued_req(pn_agrID In Number -- идентификатор договора
                        , pd_onDate in Date -- дата, на которую должны быть получены остатки
                         ) return Number
 is
   n_retval Number:= cn_Zero;
 Begin
   n_retval := Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 10  -- счет требований банка
                                                 -- ,TYPEACC2_in => 101
                                                  ,TYPEACC2_in => 40 -- счет учета накопленной комиссии
                                                  ),cn_Zero);
   -------
   n_retval := n_retval + Coalesce(cdbalance.get_CurSaldo2a(agrID_in => pn_agrID
                                                  ,defdate_in => pd_onDate
                                                  ,TYPEACC1_in => 705 -- счет приобретенных прав %%
                                                  ,TYPEACC2_in => 781 -- счет приобретенных прав комиссий
                                                  ), cn_Zero);
    ---
    Return n_retval;
 End;
 -------------------------------------------------------------------
  -- счета требований банка
 function get_accrued_req(pn_agrID In Number -- идентификатор договора
                        , pn_part In Number  -- часть
                        , pd_onDate in Date -- дата, на которую должны быть получены остатки
                         ) return Number
 is
   n_retval Number:= cn_Zero;
 Begin
   n_retval := n_retval + cdbalance.get_CurSaldo2a( agrID_in => pn_agrID
                                                  , defdate_in => pd_onDate
                                                  , PART_in => pn_part
                                                  , TYPEACC1_in => 10  -- счет требований банка
                                                 -- ,TYPEACC2_in => 101
                                                  , TYPEACC2_in => 40 -- счет учета накопленной комиссии
                                                  );
   -------
   n_retval := n_retval + cdbalance.get_CurSaldo2a( agrID_in => pn_agrID
                                                  , defdate_in => pd_onDate
                                                  , PART_in => pn_part
                                                  , TYPEACC1_in => 705 -- счет приобретенных прав %%
                                                  , TYPEACC2_in => 781 -- счет приобретенных прав комиссий
                                                  );
    ---
    Return n_retval;
 End;
 -------------------------------------------------------------------
 -------------------------------------------------------------------
  -- отладочная печать потока в dbms_output
  Procedure Print_Potok(pc_prefix in Varchar2
                       , a_potok_xnpv in cd_types.t_pltab_potok2
                       , pn_isdbms In Integer --Default 0 -- 1 - формировать dbms_output, 0 - не выводить dbms_output
                       )
  Is

  Begin
    IF pn_isdbms = ci_ISDBMS
      THEN
      db_out(pc_prefix => pc_prefix
           , pc_text => ' печать потока:'
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
 -- Vct 09.10.2019 - определение суммы клиентского платежа на интервале
 -- есть похожая ф-ция в cdsytate2.Get_ClientPay_LPCCOM...
 Function Get_CliPay_ByPart(p_AGRID in Number  -- pn_agrId
                          , p_ipart in Number -- => pn_part  -- номер части
                          , p_DFrom in Date -- => (d_prevevt_date + ci_One)
                          , p_DTO  in Date -- => pd_evt_date
                          ) Return Number
 Is
 nRetVal Number := cn_Zero;
 Begin
   ---
   select
      SUM(e.cli_out) cli_out              -- заплачено клиентом (включая возврат ОД)
--      SUM(e.inp+e.cli_out_pc+e.cli_out_com) cli_out              -- заплачено клиентом (возврат ОД, %, комисии)
   into nretval
      from v_cde0 e
   where e.dog=p_AGRID
   and e.part =  p_ipart
   and e.dat between p_DFrom and p_DTO;

   Return coalesce(nretval, cn_zero);
 End;
 -------------------------------------------------------------------
 -- Vct 09.10.2019 - определение суммы клиентского платежа на интервале
 -- есть похожая ф-ция в cdsytate2.Get_ClientPay_LPCCOM...
 Function Get_CliPay(p_AGRID in Number  -- pn_agrId
                    --      , p_ipart in Number -- => pn_part  -- номер части
                   , p_DFrom in Date -- => (d_prevevt_date + ci_One)
                   , p_DTO  in Date -- => pd_evt_date
                    ) Return Number
 Is
 nRetVal Number := cn_Zero;
 Begin
   ---
   select
      SUM(e.cli_out) cli_out              -- заплачено клиентом (включая возврат ОД)
--      SUM(e.inp+e.cli_out_pc+e.cli_out_com) cli_out              -- заплачено клиентом (возврат ОД, %, комисии)
   Into nretval
   from v_cde0 e
   where e.dog=p_AGRID
   and e.dat between p_DFrom and p_DTO;

   Return coalesce(nretval, cn_zero);
 End;
 -------------------------------------------------------------------
 -- Vct 17.10.2019 получение суммы накопленных процентов в текущем интервале
 Function get_AccruedPercent(pn_agrId In Number -- договор
                            , pc_RT IN Varchar2 -- тип расчета %%, для которого собираем сумму
                            , pd_onDate In Date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                            ) return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- не ждем no_data_found пока нет group by в запросе
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
 -- Vct 17.10.2019 получение суммы накопленных процентов в текущем интервале по части
 Function get_AccruedPercent(pn_agrId In Number -- договор
                            , pn_part In Number -- номр части
                            , pc_RT IN Varchar2 -- тип расчета %%, для которого собираем сумму
                            , pd_onDate In Date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                            ) return Number
 Is
   n_retval Number := cn_Zero;
 Begin
   -- не ждем no_data_found пока нет group by в запросе
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
 -- формирует сумму события для записи в cde
 -- принимает на вход код и сумму целевого события, а также код возможного парного события
 --
 Function bf_mk_evt_difference(pn_agrID in Number -- код договора
                              , pi_part in Integer -- часть
                              , pd_onDate in Date -- дата, в которой происходит сравнение
                              , pn_goalSum in Number
                              , pi_evt_goal in Integer
                              , pi_evt_anti in Integer
                              )
          Return Number
 Is
   ndb_sum Number;     -- сумма в cde, соответствующая pi_evt_goal
   ndb_antisum Number; -- сумма в cde, соответствующая pi_evt_anti
   nretval Number;     -- результат
   Cursor crs_cde2r(pn_agrID in Number -- код договора
                  , pi_part in Integer -- часть
                  , pd_onDate in Date -- дата, в которой происходит сравнение
                  , pi_evt_goal in Integer
                  , pi_evt_anti in Integer
                  )
   Is
   Select
     SUM(Case When e.icdetype = pi_evt_goal
            Then e.mcdesum
         Else 0.0 End) as f_sum,  -- сумма целевого события
     SUM(Case When e.icdetype = pi_evt_anti
            Then e.mcdesum
         Else 0.0 End) as f_antisum -- сумма парного события
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
   -- получение ndb_sum и ndb_antisum
   --------------------------------
   -- используется open-fetch-close в связи с ожиданием, что записей в большинстве случаев не будет обнаруживаться
   -- (исключаем no_data_found)
   Open crs_cde2r(pn_agrID => pn_agrID -- код договора
                , pi_part => pi_part -- часть
                , pd_onDate => trunc(pd_onDate) -- дата, в которой происходит сравнение
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
 -- внутренняя служебная функция определения суммы амортзационного дохода в деньгах
 Function calc_DPR_Revenue( pf_rate_in in Number -- оценочная %%я ставка (здесь - ставка амортизации)
                          , pd_startdate in Date -- дата начала интервала
                          , pd_enddate in Date   -- дата завершения интервала
                          , ppf_pmt in Number    -- сумма, по отношению к которой вычисляется доход
                                                 -- по текущему использованию, сюда подается предыдущий остаток
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
    -- сумма дохода по амортизации (счет 521, событие 400)
    Return  (n_dpr_full_money - to_money(ppf_pmt));
 End;
 -------------------------------------------------------------------
 -- Vct 22.11.2018 z.186064
 -- получение значения настройки (из фпз) указывающей, с какой стадии начинать применять коэффициент резервирования
 -- для вычисления базы амортизационного дохода
 Function get_ac_discount_from_stage(pd_onDate in date) return Number
 Is
 -- идентификатор точки входа
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
       -- вообще говоря, здеь ожидаем целое число в виде строкового представления
       -- но если окажется не целое, то должно быть отформатировано с разделителем точка (.)
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
 -- определение  суммы амортизационного дохода с учетом применения к (поданному на вход) остатку на счете амортизации
 -- правила учета коэффициента резервирования
 -- если коэффициент резервирования учитывается, исходная база дохода умножается на (1  - kr)
 Function calc_DPR_Revenue_IFRS( pn_agrID In Number -- договор, для которого происходит определение дохода
                               , pd_evtDate in Date -- "дата события" или "текущая дата", вероятно, почти всегда будет совпадать с  pd_enddate
                               , pf_rate_in in Number -- оценочная %%я ставка (здесь - ставка амортизации)
                               , pd_startdate in Date -- дата начала интервала
                               , pd_enddate in Date   -- дата завершения интервала
                               , ppf_pmt in Number    -- сумма, по отношению к которой вычисляется доход
                                                      -- по текущему использованию, сюда подается предыдущий остаток
                               , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                               ) Return Number
 Is
   n_base_money Number; -- для определения базы, с которой считаем доход
   kr Number; -- коэффициент резервирования

   n_agr_stage Number; -- стадия резервирования договора
   n_rule_stage Number; -- значение настройки, указывающее, с какой стадии начинать применять коэффициент резервирования
   --
   n_retval Number;

 Begin
   n_base_money := ppf_pmt;
   -- получить правило (номер стадии) с которой использовать учет дисконта для базы дохода
   n_rule_stage := get_ac_discount_from_stage(pd_onDate => pd_evtDate);
   -- получить ставку резервирования и стадию резервирования
   -- !! процедура может вернуть n_agr_stage и kr пустышками - null-значениями
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
   -- полученный в %% коэффициент делим на 100 (умножаем на 0.01)
   kr := ci_One_Hundredth * kr;
   --------------------
   --  при определении базы использовать правило учета коэффициента резервировани стадии
   IF n_agr_stage >= n_rule_stage
     THEN -- применяем дисконт на коэффициент резервирования
       n_base_money := (1.0 - kr) * n_base_money;
   END IF;
   IF pb_isdbms
     THEN
       cd_utl2s.TxtOut('cd_dpr.calc_DPR_Revenue_IFRS.424: n_base_money :='||n_base_money
                      );
   END IF;
   n_retval := calc_DPR_Revenue( pf_rate_in => pf_rate_in -- оценочная %%я ставка (здесь - ставка амортизации)
                          , pd_startdate => pd_startdate  -- дата начала интервала
                          , pd_enddate => pd_enddate      -- дата завершения интервала
                          , ppf_pmt => n_base_money       -- сумма, по отношению к которой вычисляется доход
                                                          -- по текущему использованию, сюда подается предыдущий остаток
                          );

   Return n_retval;
 End;
  -----------------------------------------
  -- процедура формирования следующего элемента выходного массива
  -- входные параметры соответствуют структуре T_CDE_REGEVENT_CALLPRM_RT
  -- для использования в mk_current_dpr, mk_initial_dpr
  Procedure make_next_out_array_element(
                                        icurPos In Out Pls_integer
                                      , pa_evt_queue IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                                      , p_ncdeAgrid in cde.ncdeagrid%Type      -- договор
                                      , p_icdePart in cde.icdepart%Type        -- часть
                                      , p_icdeType in cde.icdepart%Type        -- тип события
                                      , p_icdeSubType in cde.icdesubtype%Type Default Null -- подтип события
                                      , p_dcdeDate in cde.dcdedate%Type -- дата обытия
                                      , p_mcdeSum in cde.mcdesum%Type  -- сумма
                                      , p_ccdeRem in cde.ccderem%Type Default null  -- комментарий к операции
                                      , p_ncdeCZO in CDE.ncdeCZO%TYPE Default Null
                                      , pc_Declarative In CDD.CCDDNOTRN%TYPE Default cc_Declarative_event
                                       )
  Is
  Begin
    ------------------------------------------
    icurPos := icurPos + ci_One;
    pa_evt_queue(icurPos).ncdeAgrid := p_ncdeAgrid; -- договор
    pa_evt_queue(icurPos).icdePart  := p_icdePart;  -- часть
    pa_evt_queue(icurPos).icdeType  := p_icdeType;  -- тип события
    --
    pa_evt_queue(icurPos).icdeType  := p_icdeType;  -- тип события
    --
    pa_evt_queue(icurPos).icdeSubType := p_icdeSubType; -- подтип события

    pa_evt_queue(icurPos).dcdeDate  := p_dcdeDate;  -- дата события
    pa_evt_queue(icurPos).mcdeSum   := p_mcdeSum;   -- сумма
    pa_evt_queue(icurPos).ccdeRem   := p_ccdeRem;   -- комментарий к операции
    pa_evt_queue(icurPos).ncdeCZO   := p_ncdeCZO;
    -- Vct 15.10.2018
    pa_evt_queue(icurPos).cDeclarative := pc_Declarative;
    ------------------------------------------

  End;
 -------------------------------------------------------------------
 -- Vct 12.02.2019
 -- для использования при проведении начальной амортизации по частям:
 -- строит массив частей для которых необходимо проводить первичную амортизацию
 -- индекс выходного массива соответствует части, для которой требуется проведение операции
 Procedure Find_initial_Parts(pn_agr_in in Number -- номер договора
                            , pd_on_date in Date -- дата, в которой будут отыскиваться части с событиями "выдачи"
                            , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                            , pa_parts_out OUT NOCOPY cd_types.T_PLDICT_NUMBER -- массив найденных частей
                            )
 Is
   a_temp cd_types.T_PLDICT_NUMBER;
   iPos Binary_Integer; -- позиция в a_temp
   i_part Binary_Integer;
 Begin
   pa_parts_out := a_temp; --
   -- проверка необходимости ведения амортизации
   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => pc_dprcode)
     THEN
       -- амортизация по договору не ведется,
       -- у него не может быть "подходящих частей"
       -- возвращаем пустышку
       -- pa_parts_out := a_temp; -- вынесено из if
       Return; -- ранний возврат
   END IF;
   --------- получение первичного массива ------------
   Begin
     IF cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dprcode)
       THEN
       -- вариант поиска по всем частям
       Select e.icdepart
       Bulk collect into a_temp
       From cde e
       Where 1 = 1
         And e.ncdeagrid = pn_agr_in
         And e.dcdedate = Cast(pd_on_date as Date)
         And e.icdetype in (1,41,701) -- список событий, которые принимаем ак выдачи
         -- TODO - только списка частей будет недостаточно. Скорее всего потребуются доп проверки
         -- например, на наличие ненулевого остатка по 520 счету или что-то еще...
       ;

     ELSE
       -- вараинт поиска только по первой части для случая веденя по договору
       Select e.icdepart
       Bulk collect into a_temp
       From cde e
       Where 1 = 1
         And e.ncdeagrid = pn_agr_in
         And e.dcdedate = Cast(pd_on_date as Date)
         And e.icdepart = 1
         And e.icdetype in (1 -- выдача
                            ,41 -- приход с пролонгации z.194593
                           ,701 -- вероятно это покупка цессии
                            ) -- список событий, которые принимаем ак выдачи
       ;

     END IF;
   End;
   ------- преобразование "столбцов в строки" --------
   -- pa_parts_out будет проверяться, по существу, только на Exists, поэтому делаем такой переворот
   Begin
     iPos := a_temp.First();
     WHILE iPos Is Not Null
     LOOP
       i_part := a_temp(iPos);
       pa_parts_out(i_part) := 0; -- например, на единичку значение можно поменять по завершении обработки части
       --
       iPos := a_temp.Next(iPos);
     END LOOP;
   End;

 End;

 -------------------------------------------------------------------
 -- Vct 12.02.2019
 -- для использования при проведении начальной амортизации по частям:
 -- для формирования глобальной переменной с обрабатываемым договором и частью/частями, по которым
 -- необходимо проводить начальную амортизацию.
 Function create_initial_parts_descr(pn_agrid in Number -- номер договора
                            , pd_on_date in Date -- дата, в которой будут отыскиваться части с событиями "выдачи"
                           -- , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                           -- , pa_parts_out OUT cd_types.T_PLDICT_NUMBER -- массив найденных частей
                            ) Return T_REC_INITIAL_DPR_AGR_CND
 is
   vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC;
   vr_agrdpr_parts T_REC_INITIAL_DPR_AGR_CND;
 Begin

--  Clear_initialdpr_parts;

  vr_agrdpr_parts.fn_agrID := pn_agrid;

   -- признак необходимости ведения амортизации
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);
   vr_agrdpr_parts.fc_dprcode := vc_dpr_code;

   Find_initial_Parts(pn_agr_in => pn_agrid -- номер договора
                    , pd_on_date => pd_on_date -- дата, в которой будут отыскиваться части с событиями "выдачи"
                    , pc_dprcode => vc_dpr_code -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                    , pa_parts_out => vr_agrdpr_parts.a_parts -- массив найденных частей
                    );

   --gr_initialdpr_parts := vr_agrdpr_parts;
   Return vr_agrdpr_parts;
 End;
 -------------------------------------------------------------------
 -- Vct 15.04.2020 
 -- для использования в текущей амортизации
 -------------------------------------------------------------------
 -- Vct 12.02.2019 проверка необходимости обработки части
 Procedure bp_checkpart_for_initial_dpr( pn_agrid in Number
                                       , pn_part  in Number
                                       , pd_on_date in Date
                                       , pb_good_part OUT Boolean -- результат - True - часть требует обработки,
                                                                  -- False - нет - части нет среди подобранных, или она уже была обработана ранее
                                      )
 Is
 Begin
   IF NOT cd_utl2s.is_equal( gr_initialdpr_parts.fn_agrID,pn_agrid)
     THEN
       gr_initialdpr_parts := create_initial_parts_descr(pn_agrid => pn_agrid -- номер договора
                                                       , pd_on_date => pd_on_date -- дата, в которой будут отыскиваться части с событиями "выдачи"
                                                        );
   END IF;
   -- проверка существования части
   pb_good_part := gr_initialdpr_parts.a_parts.Exists(pn_part);
   -- проверка, что часть ранее не обрабатывалась (значение по индексу части равно нулю)
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
      Null; -- TODO - сообщение об ошибке
    END IF;
  End;
 -------------------------------------------------------------------
 -- Vct 13.02.2019
 -- сохранение массива рассчитанных действий в cdd
 -- Vct 31.10.2019 - убрал модификатор OUT для параметра a_result - сам массив в процедуре по факту не меняется...
 Procedure Put_Array_CDD(a_result    IN /* OUT */ cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                       , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "приоритет", считаем, что приедет при вызове из cdgrp
                       , CDACURISO   CDA.CCDACURISO%TYPE -- валюта договора, ожидаем, что приедет  при вызове из cdgrp
                       , pb_isdbms in Boolean -- признак вывода информации в dbms_output -- Vct 27.02.2019
                        )
   is
   iPos Pls_integer;
 Begin
   -----------------------------
   -- отладка
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
         -- отладка
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
  -- Vct 03.06.2019 - выделено из mk_initial_dpr_inner_bnk
  -- процедура получения суммы амортизации после потока
  -- на базе кода из mk_current_dpr
  -- для использования в mk_initial_dpr_inner_bnk, mk_initial_dpr_inner
  Procedure bp_initial_XNPV_lc( pn_agrId in Number -- договор
                      , pn_Part in Integer -- часть, пока не используется
                      , pc_extflowsqlid in Varchar2 -- идентификатор точки входа для получения потока
                      , pn_recalc_state in Pls_integer -- 1 - пересчитывать состояние, 0 - нет
                      , pc_dprcode in cd_dpr_utl.T_DO_AMRTPROC -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                     -- , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- кодировка способа вычисления даты постановки просрочки в поток
                      , pi_dpr_pecrent Number -- ставка амортизации
                      , pd_flow_date in Date -- дата, на которую следует получать поток
                      , pd_evt_date  in Date -- дата текущего события.
                      , pm_ovd_sum in Number -- сумма просрочки, которую необходимо учесть при расчете
                      ---
                      , pb_isdbms in Boolean -- Vct 03.06.2019
                      , bWrite_to_grpLog in Boolean -- Vct 03.06.2019
                      ---
                      , pm_sum_after_mn OUT Number -- сумма апортизации после потока, на неё должен выйти 520 счет
                      , pm_sum_before_mn OUT Number -- сумма до потока с учетом выдачи, это будет сумма непроцентной корректировки
                      )
  Is
    a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
    r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv
    ----
    vd_ovdDate_toclc Date; -- дата, в которую следует учитывать просрочку
    -- i_ovd_pos Pls_integer; -- позиция в потоке, в которой будет учтена просрочка
    ---------------
    m_sum_before Number;
    m_sum_after Number;
    -- -- Vct 23.11.2018  z. 186064
    m_ovd_sum_lc Number := cn_Zero;
    --b_use_ovd_in_flow Boolean; -- если true - просрочку учитывать в потоке, иначе как слагаемое
    ---------------
    --i_xnpv_pos Pls_integer:= 1; -- позиция, в которую произошла вставка.
                                -- пока считаем всегда с 1, далее определимся...
    ---------------
    b_bypart_amrt Boolean := False;
  Begin
      -------------------------------------------------------------

      -------------------------------------------------------------
      b_bypart_amrt := cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dprcode);
      -------------------------------------------------------------
      -- получить текущий поток для текущего элемента графика
      -- заменяем вызов cd_psk.get_XNPV на прямую работу с потоком

      IF b_bypart_amrt
        Then -- амортизация в разрезе частей
        cd_psk.get_XNPV_array2_by_part( pn_agrid_in => pn_agrId -- договор
                              , pn_part => pn_part -- номер части
                              , pc_extflowsqlid => pc_extflowsqlid -- идентификатор точки входа для получения потока
                              , pd_ondate => pd_flow_date -- дата, на которую получать актуальный поток.
                              , pi_recalcState => pn_recalc_state  --Default 1 -- (Vct 03.05.2018) пересчет состояния 1 - пересчитывать, 0 - нет
                              ---- Vct 09.01.2019 z.190043
                             -- , pc_TypeCalcState In  Varchar2 Default 'T' -- плановый график на дату
                                                                       -- 'R'; --реальный график на текущую дату
                              ----
                              , pa_potok_out => a_potok_xnpv -- выходной поток
                                -- структура для поддержки двоичного поиска в выходном массиве pa_potok_out
                              , pr_search_struct => r_bsearch_result
                              );

      ELSE -- амортизация в целом по договору
        -- TODO - проверить, пересчитывается ли состояние в этом вызове.
        cd_psk.get_XNPV_array2( pn_agrid_in => pn_agrId -- договор
                      , pc_extflowsqlid => pc_extflowsqlid -- идентификатор точки входа для получения потока
                      , pd_ondate => pd_flow_date -- pd_evt_date -- дата, на которую получать актуальный поток.
                      , pi_recalcState => pn_recalc_state
                      , pa_potok_out => a_potok_xnpv -- выходной поток
                        -- структура для поддержки двоичного поиска в выходном массиве pa_potok_out
                      , pr_search_struct => r_bsearch_result -- cd_psk_sutl.T_SEARCHD_CACHE 'инициализируется как выходня структура
                      );
      END IF;

      -- отладка
      IF pb_isdbms THEN
        Print_Potok(pc_prefix => 'cd_dpr.bp_initial_XNPV_lc.341: pc_dprcode='||pc_dprcode||' pn_part='||pn_part
                    , a_potok_xnpv => a_potok_xnpv
                    , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                    );
      END IF;
      ---------------------------------------------------------
      -- проверка правильности построения потока             --
      -- выбрасывает ошибку, если поток построен неправильно --
      ---------------------------------------------------------
      Begin
        cd_fcc_pkg.Check_Potok( a_potok_in => a_potok_xnpv);
      Exception
        WHEN cd_errsupport.e_DATA_CORRUPTED
          THEN
           -- поток плохой
           -- TODO - логируем ошибку
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
            , pn_isdbms => 1 -- пока будем печатать всегда. -- case pb_isdbms when True then ci_One Else ci_Zero End
            );
          -- TODO  залогировать ошибку
          -- переходим к следующему договору
          -- CONTINUE;
      End;
      ---------------------------------------------------------
      /*
      -- вставка текущей даты в поток
      -- TODO - возможно, здесь умнее было бы просто переставить стартовый индекс...
      Begin

        -----------------------------------------------------------------
        -- найти позицию в массиве, соответствующую текущей дате события
        cd_psk_sutl.bin_find_date_pos(p_array => a_potok_xnpv -- сортированный по дате поток без пропуска индексов и null значений в датах
                                    , pr_scache => r_bsearch_result  -- структура со значениями предыдущего поиска
                                    , pd_DefDate => pd_evt_date -- d_evtDate -- дата, позиция которой отыскивается в p_array
                                     );

        -- здесь в случае текущей работы (с не певым элементов должна быть проверка результата поиска)
        -- i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
        IF  r_bsearch_result.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
           OR (r_bsearch_result.iOldIndex != cd_psk_sutl.ci_SEARCH_FAILED
               And r_bsearch_result.i_exact != cd_psk_sutl.ci_MATCH_EXACT
              )
           THEN
             -- не нашлась в потоке точная дата текущего события.
             -- тогда вставляем строку с нулевой суммой в дату текущего события
          --------------------------------------------

          -- вставляем в поток строку с нулевой суммой на текущую дату
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => a_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => pd_evt_date --d_evtDate -- вставляемая дата
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- вставляемая сумма (ноль)
                                         , pi_foundPos => i_xnpv_pos -- позиция, в которую произошла вставка.
                                         );

          --------------------------------------------
          IF  i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
            THEN -- это ошибочная ситуация - сюда не должны попадать
                 -- не нашлось позиции для вставки
                 -- TODO - логируем ошибку
                 -- ...
                   db_out(pc_prefix => 'cd_dpr.bp_XNVP_lc.342:'
                        , pc_text => ' для даты <'
                        ||fmt_date_out(pd_evt_date)
                        ||'> не нашлось позиции для вставки в поток. Прерываем работу с договором '
                        ||cd_utl2s.num_to_str_dot(pn_agrid)
                         );
                   --------------------------------------------
                   -- TODO - выбросить ошибку ???          ----
                   -- переходим к следующему договору ???  ----
                   -- CONTINUE; -- ???                     ----
                   --------------------------------------------
          END IF;
          -- поправляем точку начала счета
          r_bsearch_result.iOldIndex := i_xnpv_pos;
          r_bsearch_result.i_exact := cd_psk_sutl.ci_MATCH_EXACT;
          -- поправляем размер масива в структуре поиска
          r_bsearch_result.iArrayEnd := a_potok_xnpv.Last;
        END IF;
      End;
      --
      */
      ---------------------------------------------------------
      ---------- если просрочка не нулевая, определяем место ее постановки в поток
--      IF pm_ovd_sum != cn_Zero
--        THEN
               -- Vct 23.11.2018
--        b_use_ovd_in_flow := cd_psk_sutl.Encapsulate_ovd_in_flow(pc_code_in => pc_odvclc_code);
        -- Vct 23.11.2018 - if
--        IF b_use_ovd_in_flow
--          THEN
--        -- получить дату учета просрочки в потоке
--        vd_ovdDate_toclc := cd_psk_sutl.get_ovd_date_byCode( pa_flow => a_potok_xnpv      -- поток, на котором отыскивается дата
--                                                           , pc_code => pc_odvclc_code     -- кодировка способа вычисления даты
--                                                           , pd_reper_date_in => pd_evt_date -- опорная дата
--                                                           );
        -- создаем в потоке строку с датой, соответствующей дате учета просрочки
--        cd_psk_sutl.mk_datepoint_inflow( pa_flow => a_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
--                                       , pd_Date_in => vd_ovdDate_toclc -- вставляемая дата
--                                       , pn_isum => cd_fcc_pkg.cn_Zero -- вставляемая сумма (ноль)
--                                       , pi_foundPos => i_ovd_pos -- позиция, в которую произошла вставка строки для просрочки
--                                       );
        -- поправляем размер масива в структуре поиска
--        r_bsearch_result.iArrayEnd := a_potok_xnpv.Last;

--        ELSE         -- Vct 23.11.2018 - elsif

--          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- не учитывать просрочку в потоке
--          m_ovd_sum_lc := pm_ovd_sum;

--        END IF;

--      ELSE
        -- дату учета просрочки устанавливаем в бесконечность
        vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE;
--      END IF;

      --------------------------------------------
      ----
      --------------------------------------------
      -- определим амортизацию после потока на текущем потоке начиная с текущей даты,
      -- с учетом просрочки
      cd_fcc_pkg.p_xirr_dflow_inner(a_flow_in => a_potok_xnpv -- поток для суммирования
                                   , pd_rate_in => pi_dpr_pecrent -- оценка ставки %%
                                   , pd_reperdate => pd_evt_date --d_evtDate  -- опорная дата по отношению к которой считать долю времени
                                   , pi_from => a_potok_xnpv.First --i_xnpv_pos --r_bsearch_result.iOldIndex --i_xnpv_pos -- начальный индекс для суммирования
                                   , pi_to => a_potok_xnpv.Last --r_bsearch_result.iArrayEnd  -- Правая граница массива   -- конечный индекс для суммирования
                                   , pm_sum_before => m_sum_before -- сумма включая pi_from
                                   , pm_sum_after => m_sum_after --n_dpr_correct  -- сумма за вычетом pi_from
                                 --  , pm_part_from OUT T_FCC_NUMBER -- (rem Vct 17.11.2017) Vct 13.11.2017 - часть суммы амортизации, относящаяся к строке pi_from
                                   -- для этого параметра пока непонятно что делать, для случая многократных выдач (линии+...)
                                   , pi_NonNegative_in => 0 --1 --0 -- Vct 04.10.2017 учет отрицательных сумм 1 - не учитывать, 0 брать как есть
--                    -- Vct 13.02.2018 добавляем параметры учета дополнительной суммы в указанной дате (для учета просрочек)
--                    -- дата должна присутствовать в потоке
                                  , pd_additional_date => vd_ovdDate_toclc -- дата учета просрочки
                                  , pm_additional_sum =>  pm_ovd_sum --m_ovd_ondate      -- сумма просрочки
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
 -- процедура начальной амортизации
 -- (замена исходной версии, напрямую вычисляющей xirr в cdgrp)
 Procedure mk_initial_dpr_inner( pn_agrId in Number -- договор
                         , pn_part in Integer -- часть - пока не используем...
                         , pc_dpr_code IN cd_dpr_utl.T_DO_AMRTPROC -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                         , pd_evt_date in Date -- дата, в которой проводится операция
                         , pn_calcstate In Number -- 1 - проводить расчет состояния в процедуре, 0 - нет
                         , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + NOCOPY
                         )
 Is
   i_out_index pls_integer := ci_Zero; -- индекс элемента выходного массива pa_result
   i_evt_code cde.icdetype%Type;       -- код генерируемого события
   ------------------------
   i_dpr_pecrent Number; -- ставка амортизации
  -- vd_dpr_enddate Date; -- дата завершения амортизации, пока не используем
   m_sum_after_mn Number; -- сумма апортизации после потока, на неё должен выйти 520 счет
   m_sum_before_mn Number; -- сумма до потока с учетом выдачи, это будет сумма непроцентной корректировки
   m_evt_amount Number; -- для суммы действия

  -------------------------------------
  -- vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- идентификатор точки входа для получения внешнего потока
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
  vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
  bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
  ------------------------------------------
  vc_message cd_types.T_MAXSTRING;        -- текст для информационных сообщений
  -- vc_message_text cd_types.TErrorString;  -- для текста неожиданных ошибок
  ------------------------------------------
  ------------------------------------------
 Begin
   -- признак запуска в груповом процессе
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   
   --------------------------
   ---- получение ставки и даты завершения амортизации
   Begin

      -- получить ставку амортизации.

      i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- номер части
                                                 , EffDate => pd_evt_date
                                                 , pc_code => pc_dpr_code
                                                 )/cn_100;

      -- получим дату завершения амортизации по договору, если установлена
      --vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);

      -- ранний возврат:
      -- если ставка не сформирована (нет мортизации) или равна нулю (амортизация прекращена)
      -- или дата завершения амортизации предшествует pd_evt_date,
      -- то завершаем работу процедуры
      IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
     --   OR pd_evt_date >= vd_dpr_enddate
        THEN
          vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                       ||' pn_part='||pn_part
                       ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                       ||' pc_dpr_code='||pc_dpr_code
                       ||' не установлена стака амортизации ';
                      -- ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                      -- ||' : амортизация завершена или не стартовала';

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
      ------логируем полученную ставку  -----------------------------
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
  -- получаем идентификатор точки входа для получения внешнего потока,
  -- если пустая, будет использоваться "стандартный метод"
  -- + получаем способ вычисления даты, в которую будет ставиться сумма просрочки
  Begin
      --------------------------------------------
      -- получаем поток, соответствующий начальному состоянию
      --------------------------------------------
      -----
      -- получаем идентификатор точки входа, определяющий пользовательский способ определения потока
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      -- просрочку в начальной амортизации не учитываем
      /* -----------------------
      -- получить кодировку способа учета просрочки в потоке.
      -- пока будем считать ее общей на весь пробег по договору.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
      */
  End;

   -- получение значений для непроцентной корректировки и суммы амортизации после потока
   Begin
     bp_initial_XNPV_lc( pn_agrId => pn_agrId -- договор
                      , pn_Part => pn_part -- часть, пока не используется
                      , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                      , pn_recalc_state => pn_calcstate -- 1 - пересчитывать состояние, 0 - нет
                      , pc_dprcode => pc_dpr_code -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                     -- , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- кодировка способа вычисления даты постановки просрочки в поток
                      , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                      , pd_flow_date => pd_evt_date -- дата, на которую следует получать поток
                      , pd_evt_date  => pd_evt_date -- дата текущего события.
                      , pm_ovd_sum => 0.0 -- сумма просрочки, которую необходимо учесть при расчете
                      , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                      , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                      , pm_sum_after_mn => m_sum_after_mn -- сумма апортизации после потока, на неё должен выйти 520 счет
                      , pm_sum_before_mn => m_sum_before_mn -- сумма до потока с учетом выдачи, это будет сумма непроцентной корректировки
                      );
     ----- приведение сумм к деньгам
      m_sum_before_mn := to_money(m_sum_before_mn);
      m_sum_after_mn := to_money(m_sum_after_mn);

      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' до потока m_sum_before_mn :='||cd_utl2s.num_to_str_dot(m_sum_before_mn)
                   -- ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                    ||' после потока m_sum_after_mn='||cd_utl2s.num_to_str_dot(m_sum_after_mn);
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

   ----- формирование выходного массива
   Begin
     -- формирование суммы непроцентной корректировки события 401/402
     m_evt_amount := m_sum_before_mn;
     i_evt_code := Case
                    When m_evt_amount > 0 Then 401
                    When m_evt_amount < 0 Then 402
                    Else ci_Zero
                  End;
     -------------------------
     IF i_evt_code != ci_Zero
       THEN
       -- i_out_index := i_out_index + ci_One; -- rem Vct 27.02.2019 увеличение счетчика идет внутри make_next_out_array_element
       m_evt_amount := Abs(m_evt_amount);
       make_next_out_array_element(  icurPos => i_out_index
                                   , pa_evt_queue => pa_result
                                   , p_ncdeAgrid => pn_agrID      -- договор
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                   , p_icdeType => i_evt_code   -- тип события
                                   ----
                                   , p_icdeSubType => Null -- подтип события --
                                   ----
                                   , p_dcdeDate => pd_evt_date -- дата события
                                   , p_mcdeSum => m_evt_amount
                                   , p_ccdeRem => 'Амрт., непроцентная корректировка, договор '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                                  || case  cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dpr_code)
                                                       when True then ' часть '||pn_part
                                                     end
                                  , pc_Declarative => to_char(Null) -- это не декларативная проводка. Для декларативной передавать 'Y'
                                  );

     END IF;
     -- форирование действия, выводящего остаток 520 счета на ссумму pm_sum_after_mn
     m_evt_amount := m_sum_after_mn - m_sum_before_mn;
     i_evt_code := Case
                    When m_evt_amount > 0 Then 409
                    When m_evt_amount < 0 Then 410
                    Else ci_Zero
                  End;
     ----------------
     IF i_evt_code != ci_Zero
       THEN
       -- i_out_index := i_out_index + ci_One; -- Vct 27.02.2019 - увеличение индекса идет внутри make_next_out_array_element
       m_evt_amount := Abs(m_evt_amount);

       make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  ------
                                   , p_icdeSubType => 0 -- подтип события -- Vct 27.02.2019 Метим изменение остатка 520 как связанное с начальной корректировкой (подтип 0)
                                  ----
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => m_evt_amount
                                  , p_ccdeRem => 'Корректировка остатка счета амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                                  || case  cd_dpr_utl.bf_DeprecateByPart_Code(pc_amrtcode => pc_dpr_code)
                                                       when True then ' часть '||pn_part
                                                     end
                                 );
     END IF;
   End;

  ------ Ошбки здесь не перехватываются, перехват ошибок с логированием  производит вызывающий--------------------------------

 End;

-------------------------------------------------------------------
  -----------------------------------------
  -- процедура определения наличия слома графика
  Procedure get_modification_start_on_date( pn_agrid in Number -- идентификатор договора
                                        -- Vct 14.02.2019
                                        , pn_part in Integer -- номер части,
                                        , pc_dpr_code cd_dpr_utl.T_DO_AMRTPROC --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                                        --
                                        , pd_reperDate in Date -- опорная дата, на которую происходит поиск модификации
                                        , pb_isdbms in Boolean -- True - формировать dbms_output
                                        , pd_mdf_startDate_out OUT Date --дата начала действия тк
                                        )
  is
    -- история модификаций
    cd_CDR_I_ZERO_DATE constant Date := Date '1901-01-01';
    ----------------------------------------
    a_mdf_hist cd_types.T_MODIFICATION_PLTAB;
    r_bsearch_mdf_hist cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_mdf_hist
    ----------------------------------------
  Begin
    --------------------------------------------
    -- работа этой процедуры завязана на получение полной истории модификаций.
    -- с последующим поиском поданной на вход даты  в полученном массиве истории модификаций.
    -- вероятно, можно было бы сделать проще, соорудив подходящий запрос.
    -- но получение полной истории уже оказалось завязанным на точку входа,
    -- Пока, чтобы не плодить точки входа, используется поиск в массиве истории модификаций
    -- TODO - что-то придется делать для работы с частями...
    --------------------------------------------
    -- получить набор, соответствующий реструктуризациям (модификациям графиков/погашений)
    -- rem Vct 14.02.2019
    -- cd_psk.bp_get_modification_history(pn_agrid, a_mdf_hist);
    --------------------------------------------

    cd_psk.get_modification_hist_bycode(pn_agrId => pn_agrid  -- номер договора
                                     , pn_part => pn_part -- номер части
                                     , pc_code => pc_dpr_code
                                     , pa_mdf_hist_out => a_mdf_hist
                                      );

    ------------------------------------
    ----- формируем поисковую структуру --------------------
    cd_psk_sutl.mk_new_bsearch_struct(pa_potok => a_mdf_hist
                                     ,pr_search_struct => r_bsearch_mdf_hist
                                     );
    -------------
    -- отладка --
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
    -- ищем опорную дату в истории модификаций
    -- и формируем выходное значение
    Begin
      cd_psk_sutl.bin_find_date_pos(p_array => a_mdf_hist
                                , pr_scache => r_bsearch_mdf_hist
                                , pd_DefDate => pd_reperDate);
      -- и формируем выходное значение
      IF (r_bsearch_mdf_hist.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
         -- AND r_bsearch_mdf_hist.i_exact := cd_psk_sutl.ci_MATCH_FAILED
          )
          OR r_bsearch_mdf_hist.iOldIndex IS NULL -- это не должно реализоваться
        THEN   -- история может быть вообще не получена по каким-то причинам
        pd_mdf_startDate_out := cd_CDR_I_ZERO_DATE; --Date '1901-01-01';
      ELSE
        -- вероятнее всего, здесь совпадение неточное, но сейчас нас это не интересует.
        pd_mdf_startDate_out := a_mdf_hist(r_bsearch_mdf_hist.iOldIndex).dmodification;
      END IF;
    End;
    ------------
  End;

 ---------------
 -- начальная амортизация для вызова из cdgrp.Recalc_CDD_Item302 для TypeMask in ('401','402')
 -- (замена прямого вызова cd_psk.get_XNPV в cdgrp)
 Procedure mk_initial_dpr( pn_agrId in Number -- договор
                         , pn_part in Integer -- часть - пока не используем...
                         , pd_evt_date in Date -- дата, в которой проводится операция
                         , pn_calcstate In Number -- 1 - проводить расчет состояния в процедуре, 0 - нет
                         , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                         , evPrior     IN OUT CDD.ICDDPRIOR%TYPE    -- "приоритет", считаем, что приедет при вызове из cdgrp
                         , CDACURISO   CDA.CCDACURISO%TYPE -- валюта договора, ожидаем, что приедет  при вызове из cdgrp
                         )
 Is
   a_result cd_dpr.T_CDE_CALL_QUEUE_PLTAB; -- массив предполагаемых в cde операций для вставки в cdd
   --
   vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
   ------------------------------------------
   cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
   vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
   bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
   ------------------------------------------
   vc_message cd_types.T_MAXSTRING;        -- текст для информационных сообщений
   vc_message_text cd_types.TErrorString;  -- для текста неожиданных ошибок
   ------------------------------------------
   b_goodPart Boolean := False; --
 Begin
   -- проверка режима работы в автономном джобе и для такого случая отключим
   Check_DBMSOUT_Job_Mode();
   
   -- признак запуска в груповом процессе
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);

   -- признак необходимости ведения амортизации ( с учетом системной настройки)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ранний возврат, если не проставлен признак ведения амортизации на договоре
      vc_message:='по договору <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> амортизация не ведется (не выставлен признак ведения амортизации в условиях договора)';
     IF bWrite_to_grpLog
       THEN -- логируем для процесса
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
   -- проверка необходимости ведения амортизации по части
   bp_checkpart_for_initial_dpr( pn_agrid => pn_agrid
                               , pn_part  => pn_part
                               , pd_on_date => pd_evt_date
                               , pb_good_part => b_goodPart -- результат - True - часть требует обработки,
                                                          -- False - нет - части нет среди подобранных, или она уже была обработана ранее
                              );

   -- начальная амортизация, если часть признана годной
   IF b_goodPart
     THEN
     -- расчет суммы действий
     mk_initial_dpr_inner( pn_agrId => pn_agrId -- договор
                         , pn_part => pn_part -- часть - пока не используем...
                         , pc_dpr_code => vc_dpr_code -- признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                         , pd_evt_date => pd_evt_date -- дата, в которой проводится операция
                         , pn_calcstate => pn_calcstate -- 1 - проводить расчет состояния в процедуре, 0 - нет
                         , pb_isdbms => pb_isdbms -- признак вывода информации в dbms_output
                         , pa_result => a_result
                           );

     -- сохранение результата в cdd
     Put_Array_CDD(a_result    => a_result
                 , evPrior     => evPrior    -- "приоритет", считаем, что приедет при вызове из cdgrp
                 , CDACURISO   => CDACURISO -- валюта договора, ожидаем, что приедет  при вызове из cdgrp
                 , pb_isdbms => pb_isdbms -- признак вывода информации в dbms_output -- Vct 27.02.2019
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

      -- i_errorCode := SQLCODE; -- TODO - это можно было бы использовать для исключения модификации текста ошибки в каких-то "известных случаях"
                                 -- пока не используем...
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
          -- TODO: возможно, эта регистрация лишняя, Групповой процесс сам должен бы залогировать ошибку.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- заменяем код ошибки...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;
 End;
 -------------------------------------------------------------------
  -----------------------------------------
  -- Vct 03.06.2019 - выделено из кода mk_current_dpr_inner
  -- процедура получения суммы амортизации после потока
  -- для использования в процессах текущей амортизации
  Procedure bp_XNPV_lc( pn_agrId in Number -- договор
                      -- Vct 14.02.2019
                      , pn_part in Integer -- номер части,
                      , pc_dpr_code cd_dpr_utl.T_DO_AMRTPROC --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                      --
                      , pc_extflowsqlid in Varchar2 -- идентификатор точки входа для получения потока
                      , pc_odvclc_code  in cd_psk_sutl.T_OVDTIMECODE  -- кодировка способа вычисления даты постановки просрочки в поток
                      , pi_dpr_pecrent Number -- ставка амортизации
                      , pd_flow_date in Date -- дата, на которую следует получать поток
                      , pd_evt_date  in Date -- дата текущего события.
                      , pm_ovd_sum in Number -- сумма просрочки, которую необходимо учесть при расчете
                      ---
                      , pb_isdbms in Boolean -- Vct 03.06.2019
                      , bWrite_to_grpLog in Boolean -- Vct 03.06.2019
                      ---
                      , pm_sum_after_mn OUT Number
                      , pa_potok_xnpv IN OUT NOCOPY  cd_types.t_pltab_potok2 -- текущий поток для оценки амортизации после потока
                      -- Vct 28.08.2019 - признак добавления текущей даты (pd_evt_date) в поток
                      , pb_flow_modified OUT Boolean
                      -- Vct 23.09.2019
                      , i_evtdate_out_pos OUT Pls_integer -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
                      )
  Is

    r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv
    ----
    vd_ovdDate_toclc Date; -- дата, в которую следует учитывать просрочку
    i_ovd_pos Pls_integer; -- позиция в потоке, в которой бует учтена просрочка
    ---------------
    m_sum_before Number;
    m_sum_after Number;
    -- -- Vct 23.11.2018  z. 186064
    m_ovd_sum_lc Number := cn_Zero;
    b_use_ovd_in_flow Boolean; -- если true - просрочку учитывать в потоке, иначе как слагаемое
    ---------------
    i_xnpv_pos Pls_integer; -- позиция, в которую произошла вставка.
    ---------------
    b_ovd_flow_modified Boolean := False;
  Begin
      pb_flow_modified := False;

      -- TODO - ?вставить сюда расчёт состояния? -
      -------------------------------------------------------------
      -- получить текущий поток для текущего элемента графика
      -- заменяем вызов cd_psk.get_XNPV на прямую работу с потоком

      IF cd_dpr_utl.bf_DeprecateByPart_Code(pc_dpr_code)
      THEN
        cd_psk.get_XNPV_array2_by_part( pn_agrid_in => pn_agrId -- договор
                                , pn_part => pn_part -- номер части
                                , pc_extflowsqlid => pc_extflowsqlid  -- идентификатор точки входа для получения потока
                                , pd_ondate => pd_flow_date -- дата, на которую получать актуальный поток.
                                -- TODO - вычисление состояния !!!
                                -- z.203025
                                , pi_recalcState => Need_recalc_State_currentdpr() --In Pls_integer Default 1 -- (Vct 03.05.2018) пересчет состояния 1 - пересчитывать, 0 - нет
                                ---- Vct 09.01.2019 z.190043
                                -- Vct 28.02.2020 z.201909
                                , pc_TypeCalcState => 'R' -- Varchar2 Default 'T' -- плановый график на дату
                                                          -- 'R'; --реальный график на текущую дату
                                ----
                                , pa_potok_out => pa_potok_xnpv -- выходной поток
                                  -- структура для поддержки двоичного поиска в выходном массиве pa_potok_out
                                , pr_search_struct => r_bsearch_result
                                -- Vct 17.03.2020 расчёт без автономной транзакции
                                , pn_auto_transaction => 0
                                );
      -------------------------------------------------------------
      ELSE
        -- TODO - проверить, пересчитывается ли состояние в этом вызове.
        -- TODO-2 сейчас (по состоянию на 12.04.2018) всё еще не понятно, как обходиться с частями...
        cd_psk.get_XNPV_array2( pn_agrid_in => pn_agrId -- договор
                              , pc_extflowsqlid => pc_extflowsqlid -- идентификатор точки входа для получения потока
                              , pd_ondate => pd_flow_date -- pd_evt_date -- дата, на которую получать актуальный поток.
                              -- z.203025
                              ,pi_recalcState => Need_recalc_State_currentdpr()
                              , pa_potok_out => pa_potok_xnpv -- выходной поток
                              -- структура для поддержки двоичного поиска в выходном массиве pa_potok_out
                              , pr_search_struct => r_bsearch_result -- 'инициализируется как выходня структура
                              -- ой, это эксперимент...
                              -- Vct 28.02.2020 z.201909
                              , pc_TypeCalcState => 'R' -- 'T' - плановый график на дату
                              -- Vct 17.03.2020 расчёт без автономной транзакции
                              , pn_auto_transaction => 0                              
                              );
      END IF;
      ----------------------------------------------------------
      -- отладка
      ----------------------------------------------------------
      IF pb_isdbms
        THEN
        Print_Potok(pc_prefix => 'cd_dpr.bp_XNPV_lc.264:'
                    , a_potok_xnpv => pa_potok_xnpv
                    , pn_isdbms => case pb_isdbms And Not bWrite_to_grpLog When True then ci_One Else ci_Zero End -- when  pn_isdbms
                    );
      END IF;
      ---------------------------------------------------------
      -- проверка правильности построения потока             --
      -- выбрасывает ошибку, если поток построен неправильно --
      ---------------------------------------------------------
      Begin
        cd_fcc_pkg.Check_Potok( a_potok_in => pa_potok_xnpv);
      Exception
        WHEN cd_errsupport.e_DATA_CORRUPTED
          THEN
           -- поток плохой
           -- TODO - логируем ошибку
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
            , pn_isdbms => 1 -- пока будем печатать всегда. -- case pb_isdbms when True then ci_One Else ci_Zero End
            );
          -- TODO  залогировать ошибку
          -- переходим к следующему договору
          -- CONTINUE;
      End;
      ---------------------------------------------------------
      -- вставка текущей даты в поток
      -- TODO - возможно, здесь умнее было бы просто переставить стартовый индекс...
      Begin

        /* rem Vct 09.09.2019 - упрощаем код (двоичный поиск использовался дважды)...
        -----------------------------------------------------------------
        -- найти позицию в массиве, соответствующую текущей дате события
        cd_psk_sutl.bin_find_date_pos(p_array => pa_potok_xnpv -- сортированный по дате поток без пропуска индексов и null значений в датах
                                    , pr_scache => r_bsearch_result  -- структура со значениями предыдущего поиска
                                    , pd_DefDate => pd_evt_date -- d_evtDate -- дата, позиция которой отыскивается в p_array
                                     );

        -- здесь в случае текущей работы (с не певым элементов должна быть проверка результата поиска)
        -- i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
        IF  r_bsearch_result.iOldIndex = cd_psk_sutl.ci_SEARCH_FAILED
           OR (r_bsearch_result.iOldIndex != cd_psk_sutl.ci_SEARCH_FAILED
               And r_bsearch_result.i_exact != cd_psk_sutl.ci_MATCH_EXACT
              )
           THEN
             -- не нашлась в потоке точная дата текущего события.
             -- тогда вставляем строку с нулевой суммой в дату текущего события
          -- pb_flow_modified := True; -- rem Vct 09.09.2019
            --------------------------------------------

          -- вставляем в поток строку с нулевой суммой на текущую дату
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => pd_evt_date --d_evtDate -- вставляемая дата
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- вставляемая сумма (ноль)
                                         , pi_foundPos => i_xnpv_pos -- позиция, в которую произошла вставка.
                                         -- Vct 09.09.2019
                                         , pb_row_inserted => pb_flow_modified
                                         );

          --------------------------------------------
          IF  i_xnpv_pos = cd_psk_sutl.ci_SEARCH_FAILED
            THEN -- это ошибочная ситуация - сюда не должны попадать
                 -- не нашлось позиции для вставки
                 -- TODO - логируем ошибку
                 -- ...
                   db_out(pc_prefix => 'cd_dpr.bp_XNVP_lc.342:'
                        , pc_text => ' для даты <'
                        ||fmt_date_out(pd_evt_date)
                        ||'> не нашлось позиции для вставки в поток. Прерываем работу с договором '
                        ||cd_utl2s.num_to_str_dot(pn_agrid)
                         );
                   --------------------------------------------
                   -- TODO - выбросить ошибку ???          ----
                   -- переходим к следующему договору ???  ----
                   -- CONTINUE; -- ???                     ----
                   --------------------------------------------
          END IF;
          -- поправляем точку начала счета
          r_bsearch_result.iOldIndex := i_xnpv_pos;
          r_bsearch_result.i_exact := cd_psk_sutl.ci_MATCH_EXACT;
          -- поправляем размер масива в структуре поиска
          r_bsearch_result.iArrayEnd := pa_potok_xnpv.Last;
        END IF;
        */

        -- отладка
        -- TODO - убрать.
        IF pb_isdbms   THEN
          cd_utl2s.TxtOut('cd_dpr.1368.bp_XNPV_LC:cd_psk_sutl.mk_datepoint_inflow before: pa_potok_xnpv.First='
                        ||pa_potok_xnpv.First||' pa_potok_xnpv.Last='||pa_potok_xnpv.Last); -- pa_potok_xnpv
        END IF;
        --------------------------------------------------------------------------
        -- Vct 09.09.2019 - упрощаем код (получился двойной двоичный поиск...) ...
        --
        -- вставляем в поток строку с нулевой суммой на текущую дату
        cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                       , pd_Date_in => pd_evt_date --d_evtDate -- вставляемая дата
                                       , pn_isum => cd_fcc_pkg.cn_Zero -- вставляемая сумма (ноль)
                                       , pi_foundPos => i_xnpv_pos -- позиция, в которую произошла вставка.
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
      ---------- если просрочка не нулевая, определяем место ее постановки в поток
      IF pm_ovd_sum != cn_Zero
        THEN
        -- Vct 23.11.2018
        b_use_ovd_in_flow := cd_psk_sutl.Encapsulate_ovd_in_flow(pc_code_in => pc_odvclc_code);
        -- Vct 23.11.2018 - if
        IF b_use_ovd_in_flow
          THEN
          -- получить дату учета просрочки в потоке
          vd_ovdDate_toclc := cd_psk_sutl.get_ovd_date_byCode( pa_flow => pa_potok_xnpv      -- поток, на котором отыскивается дата
                                                             , pc_code => pc_odvclc_code     -- кодировка способа вычисления даты
                                                             , pd_reper_date_in => pd_evt_date -- опорная дата
                                                             );

          -- создаем в потоке строку с датой, соответствующей дате учета просрочки
          cd_psk_sutl.mk_datepoint_inflow( pa_flow => pa_potok_xnpv --t_potok -- T_DBLPARE_SET_PLSQL
                                         , pd_Date_in => vd_ovdDate_toclc -- вставляемая дата
                                         , pn_isum => cd_fcc_pkg.cn_Zero -- вставляемая сумма (ноль)
                                         , pi_foundPos => i_ovd_pos -- позиция, в которую произошла вставка строки для просрочки
                                         -- Vct 09.09.2019
                                         , pb_row_inserted => b_ovd_flow_modified
                                         );
          -- поправляем размер масива в структуре поиска
          --r_bsearch_result.iArrayEnd := pa_potok_xnpv.Last; -- rem Vct 04.10.2019 - это стало ненужным

        ELSIF cd_psk_sutl.DoNotUse_ovd_in_Flow(pc_code_in =>  pc_odvclc_code)
          THEN -- z.191811 - не используем просрочку в составе

          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- не учитывать просрочку в потоке
          m_ovd_sum_lc := cn_Zero;

        ELSE         -- Vct 23.11.2018 - elsif

          vd_ovdDate_toclc :=  cd_chdutils.CD_MAXDATE; -- не учитывать просрочку в потоке
          m_ovd_sum_lc := pm_ovd_sum;

        END IF;

      ELSE
        -- дату учета просрочки устанавливаем в бесконечность
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
      -- определим амортизацию после потока на текущем потоке начиная с текущей даты,
      -- с учетом просрочки
      cd_fcc_pkg.p_xirr_dflow_inner(a_flow_in => pa_potok_xnpv -- поток для суммирования
                                   , pd_rate_in => pi_dpr_pecrent -- оценка ставки %%
                                   , pd_reperdate => pd_evt_date --d_evtDate  -- опорная дата по отношению к которой считать долю времени
                                   -- rem Vct 04.10.2019 -- кажется, здесь была ошибка...
                                   , pi_from => i_evtdate_out_pos -- Vct 04.10.2019
                                   --, pi_from => r_bsearch_result.iOldIndex --i_xnpv_pos -- начальный индекс для суммирования
                                   , pi_to => pa_potok_xnpv.Last --r_bsearch_result.iArrayEnd  -- Правая граница массива   -- конечный индекс для суммирования
                                   , pm_sum_before => m_sum_before -- сумма включая pi_from
                                   , pm_sum_after => m_sum_after --n_dpr_correct  -- сумма за вычетом pi_from
                                 --  , pm_part_from OUT T_FCC_NUMBER -- (rem Vct 17.11.2017) Vct 13.11.2017 - часть суммы амортизации, относящаяся к строке pi_from
                                   -- для этого параметра пока непонятно что делать, для случая многократных выдач (линии+...)
                                   , pi_NonNegative_in => 1 --0 -- Vct 04.10.2017 учет отрицательных сумм 1 - не учитывать, 0 брать как есть
--                    -- Vct 13.02.2018 добавляем параметры учета дополнительной суммы в указанной дате (для учета просрочек)
--                    -- дата должна присутствовать в потоке
                                  , pd_additional_date => vd_ovdDate_toclc -- дата учета просрочки
                                  , pm_additional_sum =>  pm_ovd_sum --m_ovd_ondate      -- сумма просрочки
                                  );
      -- rem Vct 23.11.2018
      -- pm_sum_after_mn := to_money(m_sum_after );
      -- Vct 23.11.2018 z.186064
      pm_sum_after_mn := to_money(m_sum_after + m_ovd_sum_lc);

  End;

 ------------------------------------------------------------------
 -- Vct 04.06.2019
 -- функция возвращает True, если включена настройка ведения амортизации для банков,
 -- то есть на одной паре счетов вместе с начальной амортизацией
 -- False подразумевает схему для микрофинансов - амортизация на двух парах счетов.
 Function isBankDeprecationSchema Return Boolean
 is
  -- cc_DPRREVENUEBANKSCHEMA constant cd0.ccd0value%Type := '1';
   vc_param cd0.ccd0value%Type;
 Begin
   -- читается специальная настройка.
   vc_param := cdState.Get_CD0_Params(109);
   Return Coalesce((vc_param = cc_ParamValSetted /*cc_DPRREVENUEBANKSCHEMA*/) ,False);
 End;
 --------------------------------------------------------------------
 -- получение признака ведения 520 счета от учтенного амортизационного дохода
 -- или от будущего XNPV (ЧИСТНЗ)
 Function Calc520_From_RecordedIncome Return Boolean 
 is
   vc_param cd0.ccd0value%Type;
 Begin 
   vc_param := cdState.Get_CD0_Params(123); -- '0' - расчет от ЧИСТНЗ, '1' - расчет от оборотов на 521 счете (учтенный аамортизационный доход)
   Return Coalesce((vc_param = cc_ParamValSetted) ,False);
 End;  
---------------------------------------------------------------------
-- Vct 03.10.2019
-- получение суммы процентных корректировок за период
-- в целом по договору при ведении по схеме банка в целом по договору
  Function get_cdeDprPrcSubSum_BankSchm( pd_dateStart In Date -- начала временного интервала
                                       , pd_dateEnd In Date -- завершение интервала
                                       , pn_agrId In Number -- идентификатор договора
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- запрос без group by - не ждём no_data_found
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
       -- And e.icdetype+0 in (581, 582, 583, 584) -- пока это не нужно - события целые и идут подряд
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 ------------------------------------------------------
-- Vct 03.10.2019
-- получение суммы процентных корректировок за период
-- в целом по договору при ведении по схеме банка, амортизация по частям
  Function get_cdeDprPrcSubSum_BankSchm( pd_dateStart In Date -- начала временного интервала
                                       , pd_dateEnd In Date -- завершение интервала
                                       , pn_agrId In Number -- идентификатор договора
                                       , pn_part In Number  -- номер части
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- запрос без group by - не ждём no_data_found
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
        -- And e.icdetype+0 in (581, 582, 583, 584) -- пока это не нужно - события целые и идут подряд
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
-- Vct 03.10.2019
-- получение суммы процентных корректировок за период
-- в целом по договору при ведении по схеме МФО (микрофинансовая орг) в целом по договору
  Function get_cdeDprPrcSubSum_MfoSchm( pd_dateStart In Date -- начала временного интервала
                                       , pd_dateEnd In Date -- завершение интервала
                                       , pn_agrId In Number -- идентификатор договора
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- запрос без group by - не ждём no_data_found
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
       -- And e.icdetype+0 in (405, 406) -- пока это не нужно - события целые и идут подряд
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
-- Vct 03.10.2019
-- получение суммы процентных корректировок за период
-- в целом по договору при ведении по схеме МФО (микрофинансовая орг)
-- ведение амортизации в разрезе частей
  Function get_cdeDprPrcSubSum_MfoSchm( pd_dateStart In Date -- начала временного интервала
                                       , pd_dateEnd In Date -- завершение интервала
                                       , pn_agrId In Number -- идентификатор договора
                                       , pn_part In Number -- номер части
                                       ) Return Number
  Is
    nretVal Number;
  Begin
    Begin
      -- запрос без group by - не ждём no_data_found
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
       -- And e.icdetype+0 in (405, 406) -- пока это не нужно - события целые и идут подряд
      ;
    End;
    Return Coalesce(nretval, cn_Zero);
  End;
 -------------------------------------------------------------------
 -- NB - эта процедура для вызова ТОЛЬКО из тела mk_current_dpr, самостоятельно не вызывать.
 --
 -- процедура для использования  в cdgrp.Recalc_CDD_Item302
 -- для реализации группового процесса '1051' - текущая амортизация
 -- специального кода для частей пока нет - по состоянию на 10.04.2018
 -- TODO - ввести доп. параметр, требующий или отказывающий в вычислении состояния.
 -- Vct 03.06.2019 - этот код остаётся для схемы микрофинансовых компаний,
 -- когда начальная амортизация ведется на паре внутримодульных счетов (153A/152P),
 -- а текущая на паре (151A/150P)
 Procedure mk_current_dpr_bnk( pn_agrId in Number -- договор
                             , pn_part in Integer -- часть - пока не используем...
                             , pd_evt_date in Date -- дата, в которой проводится операция
                             , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                             , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                             --- z.196126 Vct 10.10.2019
                             -- параметр обязательного выведения корректировки остатка сводного счета процентных/непроцентных корректировок
                             , pb_correction_required In Boolean --Default False
                             --- Vct 15.05.2020
                             , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- режим амортизации определяет вызывающий
                             , pb_Write_to_grpLog In Boolean -- true - заполнять CDGRP.LOG_PUT(
                            -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                             )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
  i_dpr_pecrent Number; -- ставка амортизации
  vd_dpr_enddate Date;  -- дата завершения процесса амортизации
  -------------------
  -- Vct 02.10.2019 z.195805 - возвращаем получение признака слома графика в екущем дне
  -- Vct 25/07/2019 - снимаем условие на слом графика
  d_modification_start Date; -- дата начала действия текущей модификации -- Vct 25/07/2019 - снимаем условие на слом графика
  b_broken_schedule Boolean; -- флаг смены графика в текущем дне.        -- Vct 25/07/2019 - снимаем условие на слом графика
                             -- если выставлен, надо формировать сумму непроцентных корректировок
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- признак необходимости проведения непроцентной корректировки
  b_deprecation_by_part Boolean; -- флаг амортизации по частям
  m_current_contribution Number:= cn_zero; -- сумма текущих процентных корректировок учтенных в счёт будущих в плановом потоке
  --- Vct 17.10.2019
  m_accrued_revenue Number := cn_Zero; -- доход будущего периода
  ---
  m_clipay Number := cn_Zero;
  d_curctr_Start Date;  --| -- интервал времени, на котором требуется собрать сумму m_current_contribution
  d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- флаг наличия амортизации в  pd_evt_date
  d_prevevt_date Date; -- дата последнего предыдущего формирования счета амортизации после потока
  n_dbfixed_acc520 Number:= cn_zero; -- остаток счета амортизации после потока, зафиксированный в cdbal
                            -- точно в дату pd_evt_date. Для попытки работы на разности значений
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; -- учтенные в текущем периоде будущие процентные корректировки

  -------------------
  n_dpr_revenue Number:= cn_zero; -- сумма учетного дохода по амортизации
  ----
  m_ovd_ondate Number:= cn_zero; -- сумма просрочки на текущий день.
  -- m_client_pay Number; -- сумма клиентского платежа. -- пока отазались от использования
  m_fact_revenue Number:= cn_zero; -- сумма фактически полученных доходов
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- сумма амортизации после потока, на которую нужно вывести
                             -- 520 счет по завершении операции
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- идентификатор точки входа для получения внешнего потока
  vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- кодировка способа вычисления даты постановки просрочки в поток
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- код генерируемого события
  n_evt_sum Number:= cn_zero;                -- временная переменная для хранения суммы события.
  n_evt_sum2 Number:= cn_zero;               -- сумма события с учетом ранее поставленной в этом дне суммы в cde
  m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
  --
  m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.

  m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)
  ----
  m_pending_evt_sum Number := cn_zero; -- Vct 26.02.2019 -- сумма операции для непроцентной корректировки в момент проведения текущей амортизации
  ----
  a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
                                        -- Vct 25.02.2019 - вынесено из bp_XNVP_cl
  ------------------------------------------
  nmorningPareSum Number;
  imorningPareSgn pls_integer;
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
  vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
  bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- для текста неожиданных ошибок
  ------------------------------------------
  -- Vct 28.08.2019 - признак добавления текущей даты (pd_evt_date) в поток
  b_flow_modified Boolean;
  i_evtdate_pos Pls_integer; -- позиция в потоке, соответствующая текущей дате
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- предыдущий поток для оценки амортизации после потока
  -----------------------------------        -- для использования в точках слома графиков платежей
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   -- bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   --- Vct 15.05.2020
   bWrite_to_grpLog := pb_Write_to_grpLog;   
   vc_dpr_code := pc_dpr_code; -- режим амортизации определяет вызывающий
   
/*
  -- rem Vct 15.05.2020 - переносится в mk_current_dpr
   ---------------------------------
   -- признак необходимости ведения амортизации ( с учетом системной настройки)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ранний возврат, если не проставлен признак ведения амортизации на договоре
      vc_message:='по договору <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> амортизация не ведется (не выставлен признак ведения амортизации в условиях договора)';
     -- логируем для процесса
     IF bWrite_to_grpLog
       THEN -- амортизация для договора не ведется
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
       THEN -- установлен признак ведения амортизации по частям, но нет признака ведения амортизации на части
      vc_message:='договор <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> часть <'||pn_part
                         ||'> Не установлен признак вдения амортизации для части.';

     IF bWrite_to_grpLog
       THEN -- амортизация для части не ведется
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
  -- если добрались сюда,  значит признаки амортизации на договоре установлены и непротиворечивы...
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- получение ставки и даты завершения амортизации
  Begin

    -- получить ставку амортизации.
    /* rem Vct 14.02.2019
    -- TODO - здесь пока не обыгрывается возможность замены ставки амортизации во времени!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- договор
                                        , pd_evt_date -- r.DCDASIGNDATE -- дата, на которую подучаем ставку амортизации, пока на дату подписания...
                                         )/cn_100;

    -- получим дату завершения амортизации по договору, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- получить ставку амортизации.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- номер части
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- получим дату завершения амортизации по договору/части, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- договор
                                                   , pn_part => pn_part -- часть
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ранний возврат:
    -- если ставка не сформирована (нет мортизации) или равна нулю (амортизация прекращена)
    -- или дата завершения амортизации предшествует pd_evt_date,
    -- то завершаем работу процедуры
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : амортизация завершена или не стартовала';

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
  -- 2) получаем остаток по счету 520 и дату его возникновения
  Begin
    -------------------------------------------------
    -- получаем остаток счета 520 -------------------
    -- получаем остаток и его дату по счету амортизации после потока
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c учетом амортизации по частям
                              , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- сегодня уже выполнялась процедура амортизации
           -- надо как-то суметь сработать на разностях.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- получаем остаток счета 520 на предыдущий день  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c учетом амортизации по частям
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- день предшествующий заказанному
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- в pd_evt_date амортизация не запускалась.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := 'пред. остаток-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' повторный расчет='|| case b_dpr_was_today When True Then 'Да' Else 'Нет' end
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
  -- 2a получение остатка пары 153A/152P
  Begin

      -- Vct 17.10.2019
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- договор
                                                                          , pn_part => pn_part -- часть
                                                                          , pd_ondate => pd_evt_date - 1 -- дата
                                                                           );
      ELSE
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- договор
                                                                        , pd_ondate => pd_evt_date - 1 -- дата
                                                                         );
      END IF;
    ----------
    /* rem Vct 17.10.2019
    IF NOT b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
      THEN -- амортизация по договору
      nmorningPareSum := cdbalance.get_CurSaldo2m( agrID_in   => pn_agrid  -- договор
                                                 , TYPEACC1_in => 153  -- 1й складываемый(+) тип счета
                                                 , TYPEACC2_in => 152  -- 2й вычитаемый(-) тип счета
                                                 , PART_in   => Cast(NULL as Number) -- часть
                                                 , SUBTYP_in  => Cast(NULL as Number) -- подтип счета
                                                 , defdate_in  => (pd_evt_date - 1) -- дата, на которую
                                                 );
    ELSE
      -- амортизация по части
      nmorningPareSum := cdbalance.get_CurSaldo2m( agrID_in   => pn_agrid  -- договор
                                                 , TYPEACC1_in => 153  -- 1й складываемый(+) тип счета
                                                 , TYPEACC2_in => 152  -- 2й вычитаемый(-) тип счета
                                                 , PART_in   => pn_part -- часть
                                                 , SUBTYP_in  => Cast(NULL as Number) -- подтип счета
                                                 , defdate_in  => (pd_evt_date - 1) -- дата, на которую
                                                 );
    END IF;
    */
    --
    imorningPareSgn := Sign(nmorningPareSum);
    -- imorningPareSgn
    -- nmorningPareSum
  End;
  -----------------------------------------------------------
  -- Vct 02.10.2019 z.195805 - возвращаем получение признака слома графика в екущем дне
  -- 25.07.2019 - временно убиваем. (код не удалять!!!)
  ---------------------
  -- 3) определяем, был ли слом графика платежей/погашений в текущем дне
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- идентификатор договора
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- номер части,
                                  , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- опорная дата, на которую происходит поиск модификации
                                  , pb_isdbms => pb_isdbms                       -- True - формировать dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- дата начала действия тк
                                  );
    ----
    -- вероятное TODO !!! - здесь определяется вероятный перелом графика,
    -- только если он совпадает с днем, поданным на вход.
    -- как правильно обходится с ситуацией, когда перелом произошел
    -- на интервале между vd_dpr_enddate и pd_evt_date этот код не знает, и сейчас кажется,что ему и не положено знать.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' дата начала графика d_modification_start:='||fmt_date_out(d_modification_start)
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
  -- 4) определяем сумму учетного амортизационного дохода за период
  Begin
    /* -- rem Vct 22.11.2018
    -- получение учетного дохода по амортизации
    n_dpr_revenue := calc_DPR_Revenue( pf_rate_in => i_dpr_pecrent -- оценочная %%я ставка (здесь - ставка амортизации)
                                      , pd_startdate => d_prevevt_date -- дата начала интервала
                                      , pd_enddate => pd_evt_date   -- дата завершения интервала
                                      , ppf_pmt => n_dpr_full_money_prev    -- сумма, по отношению к которой вычисляется доход
                                      );
    */
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ставка резервирования и стадия пока всегда определяется для договора в целом.
    -- поэтому не передаем часть и способ ведения амортизации.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- договор, для которого происходит определение дохода
                               , pd_evtDate => pd_evt_date  -- "дата события" или "текущая дата", вероятно, почти всегда будет совпадать с  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- оценочная %%я ставка (здесь - ставка амортизации)
                               , pd_startdate => d_prevevt_date -- дата начала интервала
                               , pd_enddate => pd_evt_date   -- дата завершения интервала
                               , ppf_pmt => n_dpr_full_money_prev    -- сумма, по отношению к которой вычисляется доход
                                                      -- по текущему использованию, сюда подается предыдущий остаток
                               , pb_isdbms => true --pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' целевой учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' с d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' по pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' с суммы n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' по ставке i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
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
    -- TODO - здесь заменить переменную !!! Иначе дальше может неправильно пойти счет
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                              , pi_part => pn_part -- часть
                              , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- счет, на которое должно уйти 400у действие
    ----- формируем в выходном массиве строку для операции по 400му действию, 521 счет
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => 400   -- тип события - 400 - начисление дохода по амортизации (уходит на кредит счета 521П (амортизационный доход))
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => 'Учетный доход амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' занесенный в базу учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
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
  -- 5) начинаем разборку с определением сумм процентных корректировок
  ---------------------------------------------------------
  Begin
  ---------------------------------------------------------
  /* -- пока отказались от использования
  -- получаем сумму клиентских платежей за период d_prevevt_date - pd_evt_date
  Begin
    -- эта сумма в первоначальной версии учитывалась при вычислении амортизации после потока
    -- пока отказались от использования
    m_client_pay := CDSTATE2.Get_ClientPay_LPCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
  End;
  */
  ---------------------------------------------------------
    -- получаем сумму начисленных (!!!) --фактически полученных доходов за период
    Begin
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- код для работы по частям.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- номер части
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- в целом по договору
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
    -- 6) формируем сумму операции по вычислению процентных корректировок
    -- Vct 02.09.2019 - эта операция меняет пару 152/153 на сумму процентной корректировки
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
      m_revenue_diff := n_evt_sum; -- сумма процентной корректировки с учетом знака.
      --------------------------
      /*
      i_evt_code := Case
                      When n_evt_sum > 0 Then 405 -- D151(%%корректировка - А) - C154 (доход П)
                      When n_evt_sum < 0 Then 406 -- D155(расход A) - C150 (%% корректировка П)
                      Else 0
                    End;
      */
      --------------------------
      -- Vct 05.06.2019
      -- imorningPareSgn
      -- nmorningPareSum
      i_evt_code := Case
                      When  imorningPareSgn = 1 -- активный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                       Then
                         -- D 153 - C 154
                         581
                      When imorningPareSgn = 1 -- активный остаток на паре
                         And n_evt_sum < 0.0 --расходная корректировка
                       Then
                         -- D155 - C 153
                         582
                      When imorningPareSgn = -1 -- пассивный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                         Then
                         -- D152 - C154
                         583
                      When imorningPareSgn = -1 -- пассивный остаток на паре
                        And n_evt_sum < 0.0 -- расходная корректировка
                         Then
                          -- D155 - C152
                          584
                      When imorningPareSgn = 0 -- активный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                         Then
                           -- D 153 - C 154
                           581
                      When imorningPareSgn = 0 -- активный остаток на паре
                        And n_evt_sum < 0.0 -- расходная корректировка
                         Then
                          -- D155 - C152
                          584
                      Else
                        0
                      End;
      ----------------------------
      n_evt_sum := Abs(n_evt_sum);
      -- 05.06.2019
      n_evt_sum2 := n_evt_sum;  -- требуется переписать закомментированный ниже кусок кода, пока просто присваиваем расчетному значению.
      ----------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (амортизационный доход) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (фактический доход) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
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
      -- корректировка суммы операции с учетом ранее поставленного в cde значения
      -- здесь теоретически может возникать отрицательное значение
      /* -- rem Vct 05.06.2019 TODO - этот кусок необходимо переработать.
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
        -- TODO - темное место, нужна доп. проверка...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
                                   , p_ncdeAgrid => pn_agrID      -- договор
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                   , p_icdeType => i_evt_code   -- тип события - 400 - начисление дохода по амортизации (уходит на счет 521)
                                   , p_dcdeDate => pd_evt_date -- дата события
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => 'Корректировка аморт. доходов договору '||cd_utl2s.num_to_str_dot(pn_agrId)  -- комментарий к операции  -- комментарий к операции
                                   -- Vct 15.10.2018 z. -- для 405/406 действие не должно быть декларативным
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
  -- Формируем суммы для выхода на правильный остаток по 520 счету
  -- вычисляем сумму непроцентных корректировок при сломе графика
  ---------------------------------------------------------
  ---------------------------------------------------------
  ---- определяем сумму просрочки в текущем дне
  Begin
    ------- найдем просрочку, приходящуюся на текущую дату (пока включая просрочку текущего дня) ----
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- идентификатор договора
                                                    , pn_part => pn_part -- часть
                                                    , pc_code => vc_dpr_code -- код способа ведения амортизации - по части, договору или не ведется
                                                    , pd_onDate => pd_evt_date -- дата, на которую определяется остаток (на вечер)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2269:'
               , '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2269:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- получаем идентификатор точки входа для получения внешнего потока,
  -- если пустая, будет использоваться "стандартный метод"
  -- + получаем способ вычисления даты, в которую будет ставиться сумма просрочки
  Begin
      --------------------------------------------
      -- получаем поток, соответствующий начальному состоянию
      --------------------------------------------
      -----
      -- получаем идентификатор точки входа, определяющий пользовательский способ определения потока
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- получить кодировку способа учета просрочки в потоке.
      -- пока будем считать ее общей на весь пробег по договору.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- определяем сумму текущей амортизации после потока,
  -- и сумму амортизации после потока по предыдущему графику
  -- далее будем вычислять проводку по счету амортизации после потока
  -- и сумму текущей непроцентной корректировки
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- сумма амортизации после потока, на которую нужно вывести
      --                       -- 520 счет по завершении операции
      --  m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
      -----------------------------------------------------------------------------------
      bp_XNPV_lc( pn_agrId => pn_agrId -- договор
                -- Vct 14.02.2019
                , pn_part => pn_part -- номер части,
                , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                --
                , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                , pc_odvclc_code  => vc_odvclc_code  -- кодировка способа вычисления даты постановки просрочки в поток
                , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                , pd_flow_date => pd_evt_date -- дата, на которую следует получать поток
                , pd_evt_date  => pd_evt_date -- дата текущего события.
                , pm_ovd_sum => m_ovd_ondate -- сумма просрочки, которую необходимо учесть при расчете
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- текущий поток для оценки амортизации после потока
                -- Vct 28.08.2019 - признак добаления текущей даты (pd_evt_date) в поток
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
                );
      ---------------------------
      -- если был слом графика, оперделяем сумму после потока по старому графику
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

     -- Vct 25/07/2019 - снимаем условие на слом графика
     -- IF b_broken_schedule
     --   THEN -- был слом графика
           -- Vct 25.02.2019 встраиваем определение суммы будущих процентных корректировок
           -- по текущему (новому) графику определяем сумму будущих процентных корректировок
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- входной поток
                                     , pn_rate => i_dpr_pecrent  -- ставка (амортизации), оценивающая поток
                                     , pd_reper_date => pd_evt_date -- опорная дата, по отношению к которой считается сумма будущих процентных корректировок
                                     -- Vct 05.03.2019 , Vct case 28/08/2019 z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                     -- Vct 11.10.2019 z.196254 - всегда вперед
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- полученная сумма будущих процентных корректировок с учетом знака.
                                     );

       /* -- rem Vct 26.02.2019
           -- получение суммы после потока по старому графику
        bp_XNPV_lc( pn_agrId => pn_agrId -- договор
                  -- Vct 14.02.2019
                  , pn_part => pn_part -- номер части,
                  , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                  --
                  , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                  , pc_odvclc_code  => vc_odvclc_code  -- кодировка способа вычисления даты постановки просрочки в поток
                  , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                  , pd_flow_date => (d_modification_start - 1) -- дата, на которую следует получать поток
                  , pd_evt_date  => pd_evt_date -- дата текущего события.
                  , pm_ovd_sum => m_ovd_ondate -- сумма просрочки, которую необходимо учесть при расчете
                  , pm_sum_after_mn => m_dpr_after_prev
                  );
                  -- встраиваем определение суммы будущих процентных корректировок
      ELSE
        -- слома графика не было.
        -- сумму по предыдущему графику считаем равной текущей сумме после потока
        m_dpr_after_prev := m_dpr_after_curent;
       */
  --    END IF; -- Vct 25/07/2019 - снимаем условие на слом графика
  End;
  ----------------------------------------------------------------
  --------------------------------------------------------------
  -- формируем операцию по выведению сумм непроцентных корректировок (403/404)
  -- TODO !!! - проверка правильности положения сумм.
  Begin

  -- Vct z.195805 - формировать непроцентные корректировки только:
  -- а) при начальной амортизации -- TODO - пока расчитываем, что начальная амортизация ведется собственным процессом...
  -- б) при формировании новгого графика
  -- b) при платежах клиента не в дату планового графика
    --
    -- Vct 09.10.2019
    IF b_flow_modified
       And (Not Coalesce(pb_correction_required, False)) --сама  сумма клиентского платежа пока не используется иначе как для принятия решения о коррекции остатка непроц.
                                                         -- поэтому не будем ее вычислять, если явно заказали выполнение коррекции
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- номер части
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

    -- Vct 02.10.2019 z.195805 -- признак необходимости проведения непроцентной корректировки
    b_need_pareCorrection := pb_correction_required -- заказали принудительно выводить остатки счета непроцентных корректировок
                       Or b_broken_schedule -- сегодня сломан график
                       OR (b_flow_modified
                          And m_clipay != cn_zero -- m_fact_revenue != cn_Zero -- z.19805 - оценку непроцентных корректировок проводим только при наличии платежей клента на внеплановую дату
                          -- z.196758 в предшествующую дату по лановому графику не было выведения 520го счета...
                          -- (может быть придется и переформулировать это условие...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                          --    And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate)
                              )
                          )
                          ;  -- клиентский платеж не дату планового графика, пока опознаем по признаку исходного отсутствия текущей даты в потоке


   IF b_need_pareCorrection
     THEN

     -- Vct 17.10.2019
     IF b_flow_modified
       THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );
          -----
          /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- договор
                                               , pn_part => pn_part
                                              , pc_RT => 'T' -- тип расчета %%, для которого собираем сумму
                                                             -- пока ожидаем, что расчет прошел по T? дальше посмотрим
                                              , pd_onDate => pd_evt_date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                                              );
         */
        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );
         /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- договор
                                                , pc_RT => 'T' -- тип расчета %%, для которого собираем сумму
                                                               -- пока ожидаем, что расчет прошел по T? дальше посмотрим

                                                , pd_onDate => pd_evt_date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                                                );
        */
        END IF;

     END IF;

     /* rem Vct 11.10.2019 z.196254 - отказываемся пока от этого вычисления...
     -- Vct z.195805 02.10.2019
     -- вычисляем размер процентных корректировок в интервале между элементами планового потока
     IF b_flow_modified
       THEN
         -- определить интервал учета текущих процентных корректировок
       --  d_curctr_Start Date;  --| -- интервал времени, на котором требуется собрать сумму m_current_contribution
        IF i_evtdate_pos > 1 -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
          THEN

           d_curctr_Start :=   a_potok_xnpv(i_evtdate_pos - 1).ddate;
           d_curctr_Start := d_curctr_Start + 1; -- следующий день от "предыдущего" элемента в потоке
        Else
           d_curctr_Start := cd_chdutils.cd_zero_cd_date; -- от начала веков
        END IF;
       ----
       d_curctr_End := pd_evt_date - 1;    -- предыдущий день от поданного на вход расчётного дня
       ----
       IF  b_deprecation_by_part
         THEN
         -- считаем по части
         m_current_contribution := get_cdeDprPrcSubSum_BankSchm( pd_dateStart => d_curctr_Start -- начала временного интервала
                                                               , pd_dateEnd => d_curctr_End -- завершение интервала
                                                               , pn_agrId => pn_agrid  -- идентификатор договора
                                                               , pn_part => pn_part -- номер части
                                                               );

       ELSE
         -- по договору в целом
         m_current_contribution := get_cdeDprPrcSubSum_BankSchm( pd_dateStart => d_curctr_Start -- начала временного интервала
                                                               , pd_dateEnd => d_curctr_End  -- завершение интервала
                                                               , pn_agrId => pn_agrid  -- идентификатор договора
                                                               );
       END IF;

     END IF;
     */
     ----------------------------------------------
-- Vct 25/07/2019 - снимаем условие на слом графика
-- Vct 25.02.2019 -- сумму непроцентных корректировок выводим только если сегодня был слом графика
--    IF b_broken_schedule   -- Vct 26.02.2019
--      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      ------------------------
      /* rem Vct 02.10.2019
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' слом графика с изменением оценки потока m_dpr_after_curent:='||cd_utl2s.num_to_str_dot(m_dpr_after_curent)
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
      -- Непроцентная корректировка при смене графика.
      -- фрагмент был долго закомментирован, код в первоначальной версии открыт Vct 25.12.2018
      -------------------------------------------------------------------
      -- тут неясно, попробуем пока m_dpr_after_curent - m_dpr_after_prev
      -- n_evt_sum := m_dpr_after_curent - m_dpr_after_prev; -- rem Vct 26.02.2019

      ----------------------------------------------------------
      -- Vct 25.02.2019 меняем способ расчета непроцентной корректировки
      -- теперь будем считать как разницу между текущим непокрытым остатком составного из двух пар счета
      -- учета незакрытого остатка непроцентной корректировки и суммой будущих процентных корректировок

      -- m_pending_diff -- сумма будущих процентных корректировок
      -- если m_pending_diff отрицательна, то мы закрываем оставшийся доход, если положительна то расход

      -- m_revenue_diff Number; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
      -- m_pending_diff Number; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)

      -- сумма сложного счета в cdbal ((153-152)-(150-151))
      -- на предшествующую дату
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- договор
                                                                          , pn_part => pn_part -- часть
                                                                          , pd_ondate => pd_evt_date - 1 -- дата
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- договор
                                                                        , pd_ondate => pd_evt_date - 1 -- дата
                                                                         );
      END IF;

      -- отладка... TODO.. дописать условие вывода...
      vc_message := ' m_pending_current_rest before  ='||m_pending_current_rest
                  ||' m_revenue_diff='|| m_revenue_diff -- -- сумма процентной корректировки с учетом знака.
                  ||' (m_pending_current_rest + m_revenue_diff)='||(m_pending_current_rest + m_revenue_diff)
                  ||' m_pending_diff='||m_pending_diff  -- оценка будущего остатка счет непроцентных корректировок
                  ||' m_current_contribution = '||m_current_contribution
                  ||' d_curctr_Start='||to_char(d_curctr_Start,'DD.MM.YYYY')
                  ||' d_curctr_End='||to_char(d_curctr_End,'DD.MM.YYYY')
                  ||' m_accrued_revenue='||m_accrued_revenue
                  ;
      db_out('cd_dpr.mk_current_dpr_bnk.2431'
             , vc_message
            );

      -- добавляем сумму сегодняшних процентных корректировок
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- в сумме будущих корректировок учитываем доходы будущего периода
      m_pending_diff := m_pending_diff + m_accrued_revenue;

       /* rem Vct 11.10.2019 z.196254
      -- 05.09.2019 Vct - вычитаем процентную корректировку текущего дня из оценки будущих кооректировок
      -- для случая когда в текущий поток была принудительно вставлена ссегодняшняя строка
      -- (интерпретируем как отсутствие планового потока в текущем дне.)
      IF b_flow_modified
        THEN
        -- 02.10.2019 z.195805 здесь еще учесть сумму процентных корректировок
        --m_pending_current_rest := m_pending_current_rest + m_current_contribution;
        --------------------------------------------------------------------------
        -- от предыдущей плановой даты...
         m_pending_diff := m_pending_diff - m_revenue_diff - m_current_contribution;
         vc_message := ' m_pending_diff='||m_pending_diff;
        db_out('cd_dpr.mk_current_dpr_bnk.2471'
             , vc_message
            );
      END IF;
      */
      m_pending_evt_sum := cn_Zero;

      -- вычисляем сумму непроцентной корректировки
      IF    m_pending_current_rest > cn_Zero -- ранее был доход
        And m_pending_diff <= cn_Zero        -- далее тоже будет доход
        THEN -- предыдущая корректировка фиксировала доход и оставшаяся сумма тоже доходная
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ранее был расход
        And m_pending_diff >= cn_Zero         -- впереди тоже расход
        THEN -- предыдущая корректировка фиксировала расход и оставшаяся сумма тоже расходная
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ранее был доход
        And m_pending_diff > cn_Zero         -- впереди расход
        THEN -- меняется знак с дохода на расход
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ранее был расход
         And m_pending_diff < cn_Zero        -- далее будет доход
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;
     ----- z.194702 Vct 26.07.2019
      i_evt_code := Case
                    When imorningPareSgn = 1 -- активный остаток на паре
                      Then
                      Case
                      When n_evt_sum > cn_Zero Then 401 --404 --401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 403 --402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    When imorningPareSgn = -1 -- пассивный остаток на паре
                      Then
                      -- TODO - тут мог перепутать направление действий - надо проверять.
                      Case
                      When n_evt_sum > cn_Zero Then 404 --401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    Else  -- нулевой остаток на паре
                      Case
                      When n_evt_sum > cn_Zero Then  401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    End;

      n_evt_sum := Abs(n_evt_sum);
      IF i_evt_code != ci_Zero
        And n_evt_sum != cn_Zero
        THEN
        make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_evt_sum
                                  , p_ccdeRem => 'Текущая непроцентная корректировка ам. по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 -- Vct 15.10.2018 z. -- для 403/404 действие не должно быть декларативным
                                 , pc_Declarative => to_char(Null)
                                 );
        END IF;


--      END IF; -- Vct 25/07/2019 - снимаем условие на слом графика
    END IF; -- Vct 02.10.2019
  End;
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- формируем операцию по выведению остатка на 520 счете (событие 409 для положительных сумм, 410 - для отрицательных)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- код генерируемого события
    --  n_evt_sum Number;                -- временная переменная для хранения суммы события.

-- Vct 25/07/2019 - снимаем условие на слом графика
--    IF b_broken_schedule   -- Vct 26.02.2019
--      -- Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN -- !!!! здесь кривата что-то сделать!
      -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + (m_dpr_after_curent - m_dpr_after_prev); -- правая часть это сумма непроцентной корректировки
      -- Vct 26/02/2016
     
      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum;
      
--      -- Vct 05.10.2019
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    ELSE
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    END IF;

    -- Vct 22.06.2018
    -- 520 оказался активным.
    -- исправляем корреспонденцию
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' коррект. остатка ам. (расчет) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
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
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
      -- TODO - темное место, нужна доп. проверка...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
      vc_message := ' коррект. остатка ам. (в базу:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
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
    ----- формируем в выходном массиве строку для операции по 409/410му действию, 520 счет
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => 'Корректировка текущего остатка суммы амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - добавляем перехват неожиданных ошибок z.190837
  -- с целью добавления информации о договоре/части/дате
  -- Имхо, это мог бы делать вызывающий...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - это можно было бы использовать для исключения модификации текста ошибки в каких-то "известных случаях"
                                 -- пока не используем...
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
          -- TODO: возможно, эта регистрация лишняя, Групповой процесс сам должен бы залогировать ошибку.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- заменяем код ошибки...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;
 -------------------------------------------------------------------
  Procedure mk_current_dpr_bnk_fv( pn_agrId in Number -- договор
                             , pn_part in Integer -- часть - пока не используем...
                             , pd_evt_date in Date -- дата, в которой проводится операция
                             , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                             , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB
                             --- z.196126 Vct 10.10.2019
                             -- параметр обязательного выведения корректировки остатка сводного счета процентных/непроцентных корректировок
                             , pb_correction_required In Boolean --Default False
                             --- Vct 15.05.2020
                             , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- режим амортизации определяет вызывающий
                             , pb_Write_to_grpLog In Boolean -- true - заполнять CDGRP.LOG_PUT(
                            -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                             )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
  i_dpr_pecrent Number; -- ставка амортизации
  vd_dpr_enddate Date;  -- дата завершения процесса амортизации
  -------------------
  -- Vct 02.10.2019 z.195805 - возвращаем получение признака слома графика в екущем дне
  -- Vct 25/07/2019 - снимаем условие на слом графика
  d_modification_start Date; -- дата начала действия текущей модификации -- Vct 25/07/2019 - снимаем условие на слом графика
  b_broken_schedule Boolean; -- флаг смены графика в текущем дне.        -- Vct 25/07/2019 - снимаем условие на слом графика
                             -- если выставлен, надо формировать сумму непроцентных корректировок
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- признак необходимости проведения непроцентной корректировки
  b_deprecation_by_part Boolean; -- флаг амортизации по частям
  
  --m_current_contribution Number:= cn_zero; -- сумма текущих процентных корректировок учтенных в счёт будущих в плановом потоке
  
  --- Vct 17.10.2019
  --m_accrued_revenue Number := cn_Zero; -- доход будущего периода
  ---
  --m_clipay Number := cn_Zero;
  --d_curctr_Start Date;  --| -- интервал времени, на котором требуется собрать сумму m_current_contribution
  --d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- флаг наличия амортизации в  pd_evt_date
  d_prevevt_date Date; -- дата последнего предыдущего формирования счета амортизации после потока
  n_dbfixed_acc520 Number:= cn_zero; -- остаток счета амортизации после потока, зафиксированный в cdbal
                            -- точно в дату pd_evt_date. Для попытки работы на разности значений
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; -- учтенные в текущем периоде будущие процентные корректировки
  -- Vct 07.04.2021 
  m_520plan Number := cn_Zero; -- плановый остаток 520го счета, на который нужно вывести значение после потока (520) счет, счетом от дохода.
  m4correction Number := cn_Zero; -- общая сумма всех корректировок, меняющая 520й счет с четом знака, для сложения с m_520plan.
  -------------------
  n_dpr_revenue Number:= cn_zero; -- сумма учетного дохода по амортизации
  ----
  --m_ovd_ondate Number:= cn_zero; -- сумма просрочки на текущий день.

  m_fact_revenue Number:= cn_zero; -- сумма фактически полученных доходов
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- сумма амортизации после потока, на которую нужно вывести
                             -- 520 счет по завершении операции
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
  -----------------------------------------
  --vc_extflowsqlid cd_mda.cmda_ac828%Type; -- идентификатор точки входа для получения внешнего потока
  --vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- кодировка способа вычисления даты постановки просрочки в поток
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- код генерируемого события
  n_evt_sum Number:= cn_zero;                -- временная переменная для хранения суммы события.
  n_evt_sum2 Number:= cn_zero;               -- сумма события с учетом ранее поставленной в этом дне суммы в cde
  --m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
  --
  --m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.

  --m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)
  ----
  --m_pending_evt_sum Number := cn_zero; -- Vct 26.02.2019 -- сумма операции для непроцентной корректировки в момент проведения текущей амортизации
  ----
  --a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
                                        -- Vct 25.02.2019 - вынесено из bp_XNVP_cl
  ------------------------------------------
  nmorningPareSum Number;
  imorningPareSgn pls_integer;
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
  vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
  bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- для текста неожиданных ошибок
  ------------------------------------------
  -- Vct 28.08.2019 - признак добавления текущей даты (pd_evt_date) в поток
  --b_flow_modified Boolean;
  --i_evtdate_pos Pls_integer; -- позиция в потоке, соответствующая текущей дате
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- предыдущий поток для оценки амортизации после потока
  -----------------------------------        -- для использования в точках слома графиков платежей
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   -- bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   --- Vct 15.05.2020
   bWrite_to_grpLog := pb_Write_to_grpLog;   
   vc_dpr_code := pc_dpr_code; -- режим амортизации определяет вызывающий
   
  -----------------------------------------------------------------------            
  -- Vct 02.10.2019
  -- если добрались сюда,  значит признаки амортизации на договоре установлены и непротиворечивы...
  -- (см mk_current_dpr)
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- получение ставки и даты завершения амортизации
  Begin

    -- получить ставку амортизации.
    /* rem Vct 14.02.2019
    -- TODO - здесь пока не обыгрывается возможность замены ставки амортизации во времени!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- договор
                                        , pd_evt_date -- r.DCDASIGNDATE -- дата, на которую подучаем ставку амортизации, пока на дату подписания...
                                         )/cn_100;

    -- получим дату завершения амортизации по договору, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- получить ставку амортизации.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- номер части
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- получим дату завершения амортизации по договору/части, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- договор
                                                   , pn_part => pn_part -- часть
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ранний возврат:
    -- если ставка не сформирована (нет мортизации) или равна нулю (амортизация прекращена)
    -- или дата завершения амортизации предшествует pd_evt_date,
    -- то завершаем работу процедуры
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : амортизация завершена или не стартовала';

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
  -- 2) получаем остаток по счету 520 и дату его возникновения
  Begin
    -------------------------------------------------
    -- получаем остаток счета 520 -------------------
    -- получаем остаток и его дату по счету амортизации после потока
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c учетом амортизации по частям
                              , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- сегодня уже выполнялась процедура амортизации
           -- надо как-то суметь сработать на разностях.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- получаем остаток счета 520 на предыдущий день  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c учетом амортизации по частям
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- день предшествующий заказанному
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- в pd_evt_date амортизация не запускалась.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := 'пред. остаток-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' повторный расчет='|| case b_dpr_was_today When True Then 'Да' Else 'Нет' end
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
  -- 2a получение остатка пары 153A/152P
  Begin

      -- Vct 17.10.2019
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- договор
                                                                          , pn_part => pn_part -- часть
                                                                          , pd_ondate => pd_evt_date - 1 -- дата
                                                                           );
      ELSE
        nmorningPareSum := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- договор
                                                                        , pd_ondate => pd_evt_date - 1 -- дата
                                                                         );
      END IF;
      
      imorningPareSgn := Sign(nmorningPareSum);

  End;
  -----------------------------------------------------------
  -- Vct 02.10.2019 z.195805 - возвращаем получение признака слома графика в екущем дне
  -- 25.07.2019 - временно убиваем. (код не удалять!!!)
  ---------------------
  -- 3) определяем, был ли слом графика платежей/погашений в текущем дне
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- идентификатор договора
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- номер части,
                                  , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- опорная дата, на которую происходит поиск модификации
                                  , pb_isdbms => pb_isdbms                       -- True - формировать dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- дата начала действия тк
                                  );
    ----
    -- вероятное TODO !!! - здесь определяется вероятный перелом графика,
    -- только если он совпадает с днем, поданным на вход.
    -- как правильно обходится с ситуацией, когда перелом произошел
    -- на интервале между vd_dpr_enddate и pd_evt_date этот код не знает, и сейчас кажется,что ему и не положено знать.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' дата начала графика d_modification_start:='||fmt_date_out(d_modification_start)
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
  -- 4) определяем сумму учетного амортизационного дохода за период
  Begin
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ставка резервирования и стадия пока всегда определяется для договора в целом.
    -- поэтому не передаем часть и способ ведения амортизации.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- договор, для которого происходит определение дохода
                               , pd_evtDate => pd_evt_date  -- "дата события" или "текущая дата", вероятно, почти всегда будет совпадать с  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- оценочная %%я ставка (здесь - ставка амортизации)
                               , pd_startdate => d_prevevt_date -- дата начала интервала
                               , pd_enddate => pd_evt_date   -- дата завершения интервала
                               , ppf_pmt => n_dpr_full_money_prev    -- сумма, по отношению к которой вычисляется доход
                                                      -- по текущему использованию, сюда подается предыдущий остаток
                               , pb_isdbms => true --pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' целевой учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' с d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' по pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' с суммы n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' по ставке i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
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
      -- получаем сумму начисленных (!!!) --фактически полученных доходов за период
    Begin 
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- код для работы по частям.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- номер части
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- в целом по договору
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
        
    -- Vct 07.04.2021 плановый остаток 520, на который нужно выйти с учетом всех опреаций, проведенных в корреспонденции с ним.
    m_520plan := n_dpr_full_money_prev + (n_dpr_revenue - m_fact_revenue) ;
    ---------------------------------------------------
    -- определение записываемого дохода по 521 счету с учетом проведенных сегодня ранее операций... 
    -- TODO - здесь заменить переменную !!! Иначе дальше может неправильно пойти счет
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                              , pi_part => pn_part -- часть
                              , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- счет, на которое должно уйти 400у действие
    ----- формируем в выходном массиве строку для операции по 400му действию, 521 счет
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => 400   -- тип события - 400 - начисление дохода по амортизации (уходит на кредит счета 521П (амортизационный доход))
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => 'Учетный доход амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' занесенный в базу учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
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
  -- начинаем разборку с определением сумм процентных корректировок
  ---------------------------------------------------------
    -- 6) формируем сумму операции по вычислению процентных корректировок
    -- Vct 02.09.2019 - эта операция меняет пару 152/153 на сумму процентной корректировки
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
     -- m_revenue_diff := n_evt_sum; -- сумма процентной корректировки с учетом знака будет использоваться при вычислении непроцентной коректировки
      --------------------------
      --------------------------
      -- Vct 05.06.2019
      -- imorningPareSgn
      -- nmorningPareSum
      i_evt_code := Case
                      When  imorningPareSgn = 1 -- активный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                       Then
                         -- D 153 - C 154
                         581
                      When imorningPareSgn = 1 -- активный остаток на паре
                         And n_evt_sum < 0.0 --расходная корректировка
                       Then
                         -- D155 - C 153
                         582
                      When imorningPareSgn = -1 -- пассивный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                         Then
                         -- D152 - C154
                         583
                      When imorningPareSgn = -1 -- пассивный остаток на паре
                        And n_evt_sum < 0.0 -- расходная корректировка
                         Then
                          -- D155 - C152
                          584
                      When imorningPareSgn = 0 -- активный остаток на паре
                         And n_evt_sum > 0.0 -- доходная корректировка
                         Then
                           -- D 153 - C 154
                           581
                      When imorningPareSgn = 0 -- активный остаток на паре
                        And n_evt_sum < 0.0 -- расходная корректировка
                         Then
                          -- D155 - C152
                          584
                      Else
                        0
                      End;
      ----------------------------
      n_evt_sum := Abs(n_evt_sum);
      -- 05.06.2019
      n_evt_sum2 := n_evt_sum;  -- требуется переписать закомментированный ниже кусок кода, пока просто присваиваем расчетному значению.
      ----------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (амортизационный доход) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (фактический доход) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
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
      -- корректировка суммы операции с учетом ранее поставленного в cde значения
      -- здесь теоретически может возникать отрицательное значение
      /* -- rem Vct 05.06.2019 TODO - этот кусок необходимо переработать.
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
        -- TODO - темное место, нужна доп. проверка...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
                                   , p_ncdeAgrid => pn_agrID      -- договор
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                   , p_icdeType => i_evt_code   -- тип события - 400 - начисление дохода по амортизации (уходит на счет 521)
                                   , p_dcdeDate => pd_evt_date -- дата события
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => 'Корректировка аморт. доходов договору '||cd_utl2s.num_to_str_dot(pn_agrId)  -- комментарий к операции  -- комментарий к операции
                                   -- Vct 15.10.2018 z. -- для 405/406 действие не должно быть декларативным
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
  -- Формируем суммы для выхода на правильный остаток по 520 счету
  -- вычисляем сумму непроцентных корректировок при сломе графика
  ---------------------------------------------------------
  /* rem Vct 08.04.2021 - TODO - разборку с непроцентными корректировками оставляем на потом...
  ---------------------------------------------------------
  ---- определяем сумму просрочки в текущем дне
  Begin
    ------- найдем просрочку, приходящуюся на текущую дату (пока включая просрочку текущего дня) ----
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- идентификатор договора
                                                    , pn_part => pn_part -- часть
                                                    , pc_code => vc_dpr_code -- код способа ведения амортизации - по части, договору или не ведется
                                                    , pd_onDate => pd_evt_date -- дата, на которую определяется остаток (на вечер)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr_bnk.2269:'
               , '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr_bnk.2269:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- получаем идентификатор точки входа для получения внешнего потока,
  -- если пустая, будет использоваться "стандартный метод"
  -- + получаем способ вычисления даты, в которую будет ставиться сумма просрочки
  Begin
      --------------------------------------------
      -- получаем поток, соответствующий начальному состоянию
      --------------------------------------------
      -----
      -- получаем идентификатор точки входа, определяющий пользовательский способ определения потока
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- получить кодировку способа учета просрочки в потоке.
      -- пока будем считать ее общей на весь пробег по договору.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- определяем сумму текущей амортизации после потока,
  -- и сумму амортизации после потока по предыдущему графику
  -- далее будем вычислять проводку по счету амортизации после потока
  -- и сумму текущей непроцентной корректировки
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- сумма амортизации после потока, на которую нужно вывести
      --                       -- 520 счет по завершении операции
      --  m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
      -----------------------------------------------------------------------------------
      bp_XNPV_lc( pn_agrId => pn_agrId -- договор
                -- Vct 14.02.2019
                , pn_part => pn_part -- номер части,
                , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                --
                , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                , pc_odvclc_code  => vc_odvclc_code  -- кодировка способа вычисления даты постановки просрочки в поток
                , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                , pd_flow_date => pd_evt_date -- дата, на которую следует получать поток
                , pd_evt_date  => pd_evt_date -- дата текущего события.
                , pm_ovd_sum => m_ovd_ondate -- сумма просрочки, которую необходимо учесть при расчете
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- текущий поток для оценки амортизации после потока
                -- Vct 28.08.2019 - признак добаления текущей даты (pd_evt_date) в поток
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
                );
      ---------------------------
      -- если был слом графика, оперделяем сумму после потока по старому графику
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

     -- Vct 25/07/2019 - снимаем условие на слом графика
     -- IF b_broken_schedule
     --   THEN -- был слом графика
           -- Vct 25.02.2019 встраиваем определение суммы будущих процентных корректировок
           -- по текущему (новому) графику определяем сумму будущих процентных корректировок
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- входной поток
                                     , pn_rate => i_dpr_pecrent  -- ставка (амортизации), оценивающая поток
                                     , pd_reper_date => pd_evt_date -- опорная дата, по отношению к которой считается сумма будущих процентных корректировок
                                     -- Vct 05.03.2019 , Vct case 28/08/2019 z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                     -- Vct 11.10.2019 z.196254 - всегда вперед
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- полученная сумма будущих процентных корректировок с учетом знака.
                                     );

  --    END IF; -- Vct 25/07/2019 - снимаем условие на слом графика
  End;
  */
  ----------------------------------------------------------------
  /* Rem Vct 08.04.2021- TODO - разборку с непроцентными корректировками оставляем на потом...
  --------------------------------------------------------------
  -- формируем операцию по выведению сумм непроцентных корректировок (403/404)
  -- TODO !!! - проверка правильности положения сумм.
  Begin

  -- Vct z.195805 - формировать непроцентные корректировки только:
  -- а) при начальной амортизации -- TODO - пока расчитываем, что начальная амортизация ведется собственным процессом...
  -- б) при формировании новгого графика
  -- b) при платежах клиента не в дату планового графика
    --
    -- Vct 09.10.2019
    IF b_flow_modified
       And (Not Coalesce(pb_correction_required, False)) --сама  сумма клиентского платежа пока не используется иначе как для принятия решения о коррекции остатка непроц.
                                                         -- поэтому не будем ее вычислять, если явно заказали выполнение коррекции
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- номер части
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

    -- Vct 02.10.2019 z.195805 -- признак необходимости проведения непроцентной корректировки
    b_need_pareCorrection := pb_correction_required -- заказали принудительно выводить остатки счета непроцентных корректировок
                       Or b_broken_schedule -- сегодня сломан график
                       OR (b_flow_modified
                          And m_clipay != cn_zero -- m_fact_revenue != cn_Zero -- z.19805 - оценку непроцентных корректировок проводим только при наличии платежей клента на внеплановую дату
                          -- z.196758 в предшествующую дату по лановому графику не было выведения 520го счета...
                          -- (может быть придется и переформулировать это условие...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                          --    And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate)
                              )
                          )
                          ;  -- клиентский платеж не дату планового графика, пока опознаем по признаку исходного отсутствия текущей даты в потоке


   IF b_need_pareCorrection
     THEN

     -- Vct 17.10.2019
     IF b_flow_modified
       THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );

        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );

        END IF;

     END IF;


     ----------------------------------------------
-- Vct 25/07/2019 - снимаем условие на слом графика
-- Vct 25.02.2019 -- сумму непроцентных корректировок выводим только если сегодня был слом графика
--    IF b_broken_schedule   -- Vct 26.02.2019
--      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      ------------------------
      -- Непроцентная корректировка при смене графика.
      -- фрагмент был долго закомментирован, код в первоначальной версии открыт Vct 25.12.2018
      -------------------------------------------------------------------
      -- Vct 25.02.2019 меняем способ расчета непроцентной корректировки
      -- теперь будем считать как разницу между текущим непокрытым остатком составного из двух пар счета
      -- учета незакрытого остатка непроцентной корректировки и суммой будущих процентных корректировок

      -- m_pending_diff -- сумма будущих процентных корректировок
      -- если m_pending_diff отрицательна, то мы закрываем оставшийся доход, если положительна то расход

      -- m_revenue_diff Number; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
      -- m_pending_diff Number; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)

      -- сумма сложного счета в cdbal ((153-152)-(150-151))
      -- на предшествующую дату
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- договор
                                                                          , pn_part => pn_part -- часть
                                                                          , pd_ondate => pd_evt_date - 1 -- дата
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- договор
                                                                        , pd_ondate => pd_evt_date - 1 -- дата
                                                                         );
      END IF;

      -- отладка... TODO.. дописать условие вывода...
      vc_message := ' m_pending_current_rest before  ='||m_pending_current_rest
                  ||' m_revenue_diff='|| m_revenue_diff -- -- сумма процентной корректировки с учетом знака.
                  ||' (m_pending_current_rest + m_revenue_diff)='||(m_pending_current_rest + m_revenue_diff)
                  ||' m_pending_diff='||m_pending_diff  -- оценка будущего остатка счет непроцентных корректировок
                  ||' m_current_contribution = '||m_current_contribution
                  ||' d_curctr_Start='||to_char(d_curctr_Start,'DD.MM.YYYY')
                  ||' d_curctr_End='||to_char(d_curctr_End,'DD.MM.YYYY')
                  ||' m_accrued_revenue='||m_accrued_revenue
                  ;
      db_out('cd_dpr.mk_current_dpr_bnk_fv.2431'
             , vc_message
            );

      -- добавляем сумму сегодняшних процентных корректировок
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- в сумме будущих корректировок учитываем доходы будущего периода
      m_pending_diff := m_pending_diff + m_accrued_revenue;
      m_pending_evt_sum := cn_Zero;

      -- вычисляем сумму непроцентной корректировки
      IF    m_pending_current_rest > cn_Zero -- ранее был доход
        And m_pending_diff <= cn_Zero        -- далее тоже будет доход
        THEN -- предыдущая корректировка фиксировала доход и оставшаяся сумма тоже доходная
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ранее был расход
        And m_pending_diff >= cn_Zero         -- впереди тоже расход
        THEN -- предыдущая корректировка фиксировала расход и оставшаяся сумма тоже расходная
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ранее был доход
        And m_pending_diff > cn_Zero         -- впереди расход
        THEN -- меняется знак с дохода на расход
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ранее был расход
         And m_pending_diff < cn_Zero        -- далее будет доход
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;
     ----- z.194702 Vct 26.07.2019
      i_evt_code := Case
                    When imorningPareSgn = 1 -- активный остаток на паре
                      Then
                      Case
                      When n_evt_sum > cn_Zero Then 401 --404 --401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 403 --402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    When imorningPareSgn = -1 -- пассивный остаток на паре
                      Then
                      -- TODO - тут мог перепутать направление действий - надо проверять.
                      Case
                      When n_evt_sum > cn_Zero Then 404 --401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    Else  -- нулевой остаток на паре
                      Case
                      When n_evt_sum > cn_Zero Then  401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 402 -- 404 (мфо)
                      Else ci_Zero
                      End
                    End;
      
      n_evt_sum := Abs(n_evt_sum);
      -- Vct 07.04.2021 
      -- действия 401-404 меняют остаток 520 счета.
      -- на эту величину нужно скорректировать m_520plan
      -- при определении суммы 409/410 действия
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
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_evt_sum
                                  , p_ccdeRem => 'Текущая непроцентная корректировка ам. по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 -- Vct 15.10.2018 z. -- для 403/404 действие не должно быть декларативным
                                 , pc_Declarative => to_char(Null)
                                 );
        END IF;


--      END IF; -- Vct 25/07/2019 - снимаем условие на слом графика
    END IF; -- Vct 02.10.2019
  End;
  */
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- формируем операцию по выведению остатка на 520 счете (событие 409 для положительных сумм, 410 - для отрицательных)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- код генерируемого события
    --  n_evt_sum Number;                -- временная переменная для хранения суммы события.

      -- Vct 26/02/2016
     -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum; -- rem Vct 07.04.2021
      n_evt_sum :=  n_dpr_full_money_prev - (m_520Plan + m4correction);
      
    -- Vct 22.06.2018
    -- 520 оказался активным.
    -- исправляем корреспонденцию
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' коррект. остатка ам. (расчет) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
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
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
      -- TODO - темное место, нужна доп. проверка...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
      vc_message := ' коррект. остатка ам. (в базу:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
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
    ----- формируем в выходном массиве строку для операции по 409/410му действию, 520 счет
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => 'Корректировка текущего остатка суммы амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - добавляем перехват неожиданных ошибок z.190837
  -- с целью добавления информации о договоре/части/дате
  -- Имхо, это мог бы делать вызывающий...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - это можно было бы использовать для исключения модификации текста ошибки в каких-то "известных случаях"
                                 -- пока не используем...
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
          -- TODO: возможно, эта регистрация лишняя, Групповой процесс сам должен бы залогировать ошибку.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- заменяем код ошибки...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;

 -------------------------------------------------------------------
 -- NB - эта процедура для вызова ТОЛЬКО из тела mk_current_dpr, самостоятельно не вызывать.
 --
 -- Vct 03.06.2019 - текущая амортизация по схеме для банков,
 -- когда текущая амортизация идет на той же паре счетов (153A/152P), что и начальная
  -- процедура для использования  в cdgrp.Recalc_CDD_Item302
 -- для реализации группового процесса '1051' - текущая амортизация
 -- специального кода для частей пока нет - по состоянию на 10.04.2018
 -- TODO - ввести доп. параметр, требующий или отказывающий в вычислении состояния.
 -- Vct 03.06.2019 - этот код остаётся для схемы работы банка
 -- когда и начальная и ткущая амортизация ведется на одной и той же паре внутримодульных счетов (153A/152P)
 Procedure mk_current_dpr_inner( pn_agrId in Number -- договор
                               , pn_part in Integer -- часть - пока не используем...
                               , pd_evt_date in Date -- дата, в которой проводится операция
                               , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                               , pa_result IN OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 15.07.2019 IN OUT NOCOPY
                               --- z.196126 Vct 10.10.2019
                               -- параметр обязательного выведения корректировки остатка сводного счета процентных/непроцентных корректировок
                               , pb_correction_required In Boolean --Default False      
                               --- Vct 15.05.2020
                               , pc_dpr_code in cd_dpr_utl.T_DO_AMRTPROC -- режим амортизации определяет вызывающий                               
                               , pb_Write_to_grpLog In Boolean -- true - заполнять CDGRP.LOG_PUT(                               
                               -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                               )
 Is
  -------------------
  i_out_index pls_integer := ci_Zero;
  -------------------
  vc_message cd_types.T_MAXSTRING;
  -------------------
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
  i_dpr_pecrent Number; -- ставка амортизации
  vd_dpr_enddate Date;  -- дата завершения процесса амортизации
  -------------------
--  Vct 25/07/2019 - снимаем условие на слом графика
--  d_modification_start Date; -- дата начала действия текущей модификации
------
-- Vct 25/07/2019 - снимаем условие на слом графика
--  b_broken_schedule Boolean; -- флаг смены графика в текущем дне.
                             -- если выставлен, надо формировать сумму енпроцентных корректировок
 -------------------
  -- Vct 02.10.2019 z.195805 - возвращаем получение признака слома графика в екущем дне
  -- Vct 25/07/2019 - снимаем условие на слом графика
  d_modification_start Date; -- дата начала действия текущей модификации -- Vct 25/07/2019 - снимаем условие на слом графика
  b_broken_schedule Boolean; -- флаг смены графика в текущем дне.        -- Vct 25/07/2019 - снимаем условие на слом графика
                             -- если выставлен, надо формировать сумму непроцентных корректировок
  -- Vct 02.10.2019 z.195805
  b_need_pareCorrection Boolean; -- признак необходимости проведения непроцентной корректировки
  b_deprecation_by_part Boolean; -- флаг амортизации по частям
  m_current_contribution Number := cn_Zero; -- сумма текущих процентных корректировок учтенных в счёт будущих в плановом потоке
  --
  m_accrued_revenue Number := cn_Zero; -- Vct 17.10.2019
  --
  m_clipay Number := cn_Zero;
  d_curctr_Start Date;  --| -- интервал времени, на котором требуется собрать сумму m_current_contribution
  d_curctr_End Date;    --|
  -------------------
  b_dpr_was_today Boolean := False; -- флаг наличия амортизации в  pd_evt_date
  d_prevevt_date Date; -- дата последнего предыдущего формирования счета амортизации после потока
  n_dbfixed_acc520 Number:= cn_zero; -- остаток счета амортизации после потока, зафиксированный в cdbal
                            -- точно в дату pd_evt_date. Для попытки работы на разности значений
  -------------------
  n_dpr_full_money_prev Number:= cn_zero; --

  -------------------
  n_dpr_revenue Number:= cn_zero; -- сумма учетного дохода по амортизации
  ----
  m_ovd_ondate Number:= cn_zero; -- сумма просрочки на текущий день.
  -- m_client_pay Number; -- сумма клиентского платежа. -- пока отазались от использования
  m_fact_revenue Number:= cn_zero; -- сумма фактически полученных доходов
  -----------------------------------------
  m_dpr_after_curent Number:= cn_zero; -- сумма амортизации после потока, на которую нужно вывести
                             -- 520 счет по завершении операции
  --- rem Vct 26.02.2019
  -- m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
  -----------------------------------------
  vc_extflowsqlid cd_mda.cmda_ac828%Type; -- идентификатор точки входа для получения внешнего потока
  vc_odvclc_code   cd_psk_sutl.T_OVDTIMECODE;  -- кодировка способа вычисления даты постановки просрочки в поток
  ------------------------------------------
  i_evt_code cde.icdetype%Type;    -- код генерируемого события
  n_evt_sum Number;                -- временная переменная для хранения суммы события.
  n_evt_sum2 Number;               -- сумма события с учетом ранее поставленной в этом дне суммы в cde
  m_revenue_diff Number := cn_Zero; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
  --
  m_pending_diff Number:= cn_zero; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.

  m_pending_current_rest Number:= cn_zero; -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)
  ----
  m_pending_evt_sum Number:= cn_zero; -- Vct 26.02.2019 -- сумма операции для непроцентной корректировки в момент проведения текущей амортизации
  ----
  a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
                                        -- Vct 25.02.2019 - вынесено из bp_XNVP_cl
  ------------------------------------------
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
  vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
  bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
  ------------------------------------------
  vc_message_text cd_types.TErrorString;  -- для текста неожиданных ошибок
  ------------------------------------------
  -- Vct 28.08.2019 - признак добавления текущей даты (pd_evt_date) в поток
  b_flow_modified Boolean;
  -- Vct 23.09.2019
  i_evtdate_pos Pls_integer; -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
  ------------------------------------------
  --a_potok_xnpv cd_types.t_pltab_potok2; -- текущий поток для оценки амортизации после потока
  --r_bsearch_result cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv
  --
  --a_potok_xnpv_prev cd_types.t_pltab_potok2; -- предыдущий поток для оценки амортизации после потока
  -----------------------------------        -- для использования в точках слома графиков платежей
  --r_bsearch_result_prev cd_psk_sutl.T_SEARCHD_CACHE; -- результат бинарного поиска в a_potok_xnpv_prev
  -----------------------------------------
  -----------------------------------------
 Begin

   --bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
   bWrite_to_grpLog := pb_Write_to_grpLog;
   vc_dpr_code := pc_dpr_code; -- Vct 15.04.2020
   
  /* rem Vct 15.05.2020 - перенесено в mk_current_dpr
   ---------------------------------
   -- признак необходимости ведения амортизации ( с учетом системной настройки)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);

   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ранний возврат, если не проставлен признак ведения амортизации на договоре
      vc_message:='по договору <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> амортизация не ведется (не выставлен признак ведения амортизации в условиях договора)';
     -- логируем для процесса
     IF bWrite_to_grpLog
       THEN -- амортизация для договора не ведется
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
       THEN -- установлен признак ведения амортизации по частям, но нет признака ведения амортизации на части
      vc_message:='договор <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> часть <'||pn_part
                         ||'> Не установлен признак вдения амортизации для части.';

     IF bWrite_to_grpLog
       THEN -- амортизация для части не ведется
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
  -- если добрались сюда,  значит признаки амортизации на договоре установлены и непротиворечивы...
  b_deprecation_by_part := cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code);
  ---------------------------------
  ---- получение ставки и даты завершения амортизации
  Begin

    -- получить ставку амортизации.
    /* rem Vct 14.02.2019
    -- TODO - здесь пока не обыгрывается возможность замены ставки амортизации во времени!!!
    i_dpr_pecrent := CDTERMS.get_dpr_rate(pn_agrId    -- r.NCDAAGRID   -- договор
                                        , pd_evt_date -- r.DCDASIGNDATE -- дата, на которую подучаем ставку амортизации, пока на дату подписания...
                                         )/cn_100;

    -- получим дату завершения амортизации по договору, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate(pn_agrId);
    */
    ---------
    -- получить ставку амортизации.
    i_dpr_pecrent := CDTERMS.get_dpr_rate_bycode(AgrID => pn_agrId
                                                 , pn_part => pn_part -- номер части
                                                 , EffDate => pd_evt_date
                                                 , pc_code => vc_dpr_code
                                                 )/cn_100;

    -- получим дату завершения амортизации по договору/части, если установлена
    vd_dpr_enddate := cdterms.get_dpr_EndDate_byCode(AgrID => pn_agrId -- договор
                                                   , pn_part => pn_part -- часть
                                                   , pc_code => vc_dpr_code
                                                     );

    -- ранний возврат:
    -- если ставка не сформирована (нет мортизации) или равна нулю (амортизация прекращена)
    -- или дата завершения амортизации предшествует pd_evt_date,
    -- то завершаем работу процедуры
    IF COALESCE(i_dpr_pecrent, cn_Zero) = cn_Zero
      OR pd_evt_date >= vd_dpr_enddate
      THEN
        vc_message :=  ' pn_agrId='||cd_utl2s.num_to_str_dot(pn_agrid)
                     ||' i_dpr_pecrent='||cd_utl2s.num_to_str_dot(i_dpr_pecrent)
                     ||' vd_dpr_enddate='||fmt_date_out(vd_dpr_enddate)
                     ||' vc_dpr_code='||vc_dpr_code
                     ||' : амортизация завершена или не стартовала';

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
  -- получаем остаток по счету 520 и дату его возникновения
  Begin
    -------------------------------------------------
    -- получаем остаток счета 520 -------------------
    -- получаем остаток и его дату по счету амортизации после потока
    cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                              , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                              -- Vct 14.02.2019 c учетом амортизации по частям
                              , PART => case when b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                             Then pn_part
                                             Else null
                                             End
                              , DFrom   => pd_evt_date
                              , pm_Saldo_out => n_dpr_full_money_prev
                              , pd_date_out => d_prevevt_date --
                              );

    IF d_prevevt_date = pd_evt_date
      THEN -- сегодня уже выполнялась процедура амортизации
           -- надо как-то суметь сработать на разностях.
      b_dpr_was_today := True;
      n_dbfixed_acc520 := n_dpr_full_money_prev;
      -- получаем остаток счета 520 на предыдущий день  ---------------
      cdbalance.bp_LastSaldoDate(AgrID => pn_agrId
                                , TYPEACC => cn_CDBAL_ACC_AFTER -- 520
                                -- Vct 14.02.2019 c учетом амортизации по частям
                                , PART => case when b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
                                               Then pn_part
                                               Else null
                                               End
                                , DFrom   => (pd_evt_date - ci_One)  -- день предшествующий заказанному
                                , pm_Saldo_out => n_dpr_full_money_prev
                                , pd_date_out => d_prevevt_date --
                                );
    ELSE
      -- в pd_evt_date амортизация не запускалась.
      n_dbfixed_acc520 := cn_Zero;
      b_dpr_was_today := False;
    END IF;
    -------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := 'пред. остаток-520 n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot( n_dpr_full_money_prev )
               ||' d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' повторный расчет='|| case b_dpr_was_today When True Then 'Да' Else 'Нет' end
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
  -- определяем, был ли слом графика платежей/погашений в текущем дне
  -- Vct 02.10.2019 z.195805 - возвращаем код получения информации о сломе графика
  -- Vct 25/07/2019 - снимаем условие на слом графика
  Begin
    get_modification_start_on_date( pn_agrid => pn_agrID                        -- идентификатор договора
                                   ---- Vct 14.02.2019
                                  , pn_part => pn_part -- номер части,
                                  , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                                   ----
                                  , pd_reperDate => pd_evt_date                  -- опорная дата, на которую происходит поиск модификации
                                  , pb_isdbms => pb_isdbms                       -- True - формировать dbms_output
                                  , pd_mdf_startDate_out => d_modification_start -- дата начала действия тк
                                  );
    ----
    -- вероятное TODO !!! - здесь определяется вероятный перелом графика,
    -- только если он совпадает с днем, поданным на вход.
    -- как правильно обходится с ситуацией, когда перелом произошел
    -- на интервале между vd_dpr_enddate и pd_evt_date этот код не знает.
    -- И сейчас кажется,что ему и не положено знать.
    b_broken_schedule := ( d_modification_start = pd_evt_date );
    --------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' дата начала графика d_modification_start:='||fmt_date_out(d_modification_start)
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
  -- определяем сумму учетного амортизационного дохода за период
  Begin
    /* -- rem Vct 22.11.2018
    -- получение учетного дохода по амортизации
    n_dpr_revenue := calc_DPR_Revenue( pf_rate_in => i_dpr_pecrent -- оценочная %%я ставка (здесь - ставка амортизации)
                                      , pd_startdate => d_prevevt_date -- дата начала интервала
                                      , pd_enddate => pd_evt_date   -- дата завершения интервала
                                      , ppf_pmt => n_dpr_full_money_prev    -- сумма, по отношению к которой вычисляется доход
                                      );
    */
    ----------------------------
    -- Vct 22.11.2018 z.186064
    -- ставка резервирования и стадия пока всегда определяется для договора в целом.
    -- поэтому не передаем часть и способ ведения амортизации.
    n_dpr_revenue := calc_DPR_Revenue_IFRS( pn_agrId => pn_agrId -- договор, для которого происходит определение дохода
                               , pd_evtDate => pd_evt_date  -- "дата события" или "текущая дата", вероятно, почти всегда будет совпадать с  pd_enddate
                               , pf_rate_in => i_dpr_pecrent -- оценочная %%я ставка (здесь - ставка амортизации)
                               , pd_startdate => d_prevevt_date -- дата начала интервала
                               , pd_enddate => pd_evt_date   -- дата завершения интервала
                               , ppf_pmt => n_dpr_full_money_prev    -- сумма, по отношению к которой вычисляется доход
                                                      -- по текущему использованию, сюда подается предыдущий остаток
                               , pb_isdbms => pb_isdbms And Not bWrite_to_grpLog
                                                --pb_isdbms
                               );

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' целевой учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
               ||' с d_prevevt_date:='||fmt_date_out(d_prevevt_date)
               ||' по pd_evt_date:='|| fmt_date_out(pd_evt_date)
               ||' с суммы n_dpr_full_money_prev:='||cd_utl2s.num_to_str_dot(n_dpr_full_money_prev)
               ||' по ставке i_dpr_pecrent:='||cd_utl2s.num_to_str_dot(i_dpr_pecrent);
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
    -- TODO - здесь заменить переменную !!! Иначе дальше может неправильно пойти счет
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    n_dpr_revenue := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                              , pi_part => pn_part -- часть
                              , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                              , pn_goalSum => n_dpr_revenue
                              , pi_evt_goal => 400
                              , pi_evt_anti => 0
                              );

    -- cn_CDBAL_ACC_REGISTRY_INCOME -- счет, на которое должно уйти 400у действие
    ----- формируем в выходном массиве строку для операции по 400му действию, 521 счет
    IF n_dpr_revenue != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => 400   -- тип события - 400 - начисление дохода по амортизации (уходит на счет 521)
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_dpr_revenue
                                  , p_ccdeRem => 'Учетный доход амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' занесенный в базу учетный ам доход (evt=400) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue);
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
  -- начинаем разборку с определением сумм процентных корректировок
  ---------------------------------------------------------
  Begin
  ---------------------------------------------------------
  /* -- пока отказались от использования
  -- получаем сумму клиентских платежей за период d_prevevt_date - pd_evt_date
  Begin
    -- эта сумма в первоначальной версии учитывалась при вычислении амортизации после потока
    -- пока отказались от использования
    m_client_pay := CDSTATE2.Get_ClientPay_LPCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
  End;
  */
  ---------------------------------------------------------
    -- получаем сумму начисленных (!!!) --фактически полученных доходов за период
    Begin
      IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
          -- код для работы по частям.
          m_fact_revenue := CDSTATE2.Get_Profit_PCCOM_ByPart(p_AGRID => pn_agrId
                                                           , p_ipart => pn_part  -- номер части
                                                           , p_DFrom => (d_prevevt_date + ci_One)
                                                           , p_DTO  => pd_evt_date
                                                              );
      ELSE
         -- в целом по договору
         m_fact_revenue := CDSTATE2.Get_Profit_PCCOM(pn_agrId, (d_prevevt_date + ci_One), pd_evt_date );
      END IF;
    End;
    ----------------------------------------------------------
    -- формируем сумму операции по вычислению процентных корректировок
    Begin
      --
      n_evt_sum := n_dpr_revenue - m_fact_revenue;
      m_revenue_diff := n_evt_sum; -- сумма процентной корректировки с учетом знака.
      i_evt_code := Case
                      When n_evt_sum > 0 Then 405
                      When n_evt_sum < 0 Then 406
                      Else 0
                    End;
      n_evt_sum := Abs(n_evt_sum);
      ----------------------------------------------------
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' (амортизационный доход) n_dpr_revenue:='||cd_utl2s.num_to_str_dot(n_dpr_revenue)
                    ||' (фактический доход) m_fact_revenue:='||cd_utl2s.num_to_str_dot(m_fact_revenue)
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
      -- корректировка суммы операции с учетом ранее поставленного в cde значения
      -- здесь теоретически может возникать отрицательное значение
      IF i_evt_code != ci_Zero
        THEN
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                         , pn_goalSum => n_evt_sum
                                         , pi_evt_goal => i_evt_code
                                         , pi_evt_anti => Case i_evt_code
                                                            When 405 Then 406
                                                            When 406 Then 405
                                                          End
                                          );
      ELSE
        -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
        -- TODO - темное место, нужна доп. проверка...
        n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                         , pi_part => pn_part -- часть
                                         , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
                                   , p_ncdeAgrid => pn_agrID      -- договор
                                   , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                   , p_icdeType => i_evt_code   -- тип события - 400 - начисление дохода по амортизации (уходит на счет 521)
                                   , p_dcdeDate => pd_evt_date -- дата события
                                   , p_mcdeSum => n_evt_sum2 --n_evt_sum
                                   , p_ccdeRem => 'Корректировка аморт. доходов договору '||cd_utl2s.num_to_str_dot(pn_agrId)  -- комментарий к операции  -- комментарий к операции
                                   -- Vct 15.10.2018 z. -- для 405/406 действие не должно быть декларативным
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
  -- Формируем суммы для выхода на правильный остаток по 520 счету
  -- вычисляем сумму непроцентных корректировок при сломе графика
  ---------------------------------------------------------
  ---------------------------------------------------------
  ---- определяем сумму просрочки в текущем дне
  Begin
    ------- найдем просрочку, приходящуюся на текущую дату (пока включая просрочку текущего дня)
    /*
    ----- rem vct 14.02.2019
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount( pn_AgrID_in => pn_agrId -- идентификатор договора
                                             , pd_onDate => pd_evt_date  -- (d_evtDate - ci_One) -- на утро -- дата, на которую определяется остаток (на вечер)
                                             );
    ------
    */
    ---- Vct 14.02.2019
    m_ovd_ondate := cd_psk.get_DPR_OVD_Amount_bycode( pn_AgrID_in => pn_agrId -- идентификатор договора
                                                    , pn_part => pn_part -- часть
                                                    , pc_code => vc_dpr_code -- код способа ведения амортизации - по части, договору или не ведется
                                                    , pd_onDate => pd_evt_date -- дата, на которую определяется остаток (на вечер)
                                                    );
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate);
    END IF;

    IF pb_isdbms And Not bWrite_to_grpLog
      THEN
        db_out('cd_dpr.mk_current_dpr.572:'
               , '(просрочка) m_ovd_ondate:='||cd_utl2s.num_to_str_dot(m_ovd_ondate)
              );
    ELSIF bWrite_to_grpLog
      THEN
        CDGRP.LOG_PUT('I', pn_agrid, 'cd_dpr.mk_current_dpr.572:'||vc_message, vn_TypPrtkl, cc_LOGMSGTYPEID);
    END IF;
  End;
  --------------------------------------------------------
  -- получаем идентификатор точки входа для получения внешнего потока,
  -- если пустая, будет использоваться "стандартный метод"
  -- + получаем способ вычисления даты, в которую будет ставиться сумма просрочки
  Begin
      --------------------------------------------
      -- получаем поток, соответствующий начальному состоянию
      --------------------------------------------
      -----
      -- получаем идентификатор точки входа, определяющий пользовательский способ определения потока
      vc_extflowsqlid := cdterms.get_agraltdprflow_sqlid(pn_agrid_in => pn_agrId);
      --
      -- получить кодировку способа учета просрочки в потоке.
      -- пока будем считать ее общей на весь пробег по договору.
      vc_odvclc_code := cd_psk.get_DPROVDRule(pn_agrID => pn_agrId
                                            , pd_onDate => pd_evt_date -- cd.Get_LSDATE --sysdate
                                             );
  End;
  -- определяем сумму текущей амортизации после потока,
  -- и сумму амортизации после потока по предыдущему графику
  -- далее будем вычислять проводку по счету амортизации после потока
  -- и сумму текущей непроцентной корректировки
  Begin
      ----------------------------------------------------------------------------------
      --  m_dpr_after_curent Number; -- сумма амортизации после потока, на которую нужно вывести
      --                       -- 520 счет по завершении операции
      --  m_dpr_after_prev Number;   -- сумма амортизации после потока при сломе графика
      -----------------------------------------------------------------------------------
      -- эксперимент 23.01.2019 - с коммитом...
       --   COMMIT WORK WRITE BATCH NOWAIT;
      bp_XNPV_lc( pn_agrId => pn_agrId -- договор
                -- Vct 14.02.2019
                , pn_part => pn_part -- номер части,
                , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                --
                , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                , pc_odvclc_code  => vc_odvclc_code  -- кодировка способа вычисления даты постановки просрочки в поток
                , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                , pd_flow_date => pd_evt_date -- дата, на которую следует получать поток
                , pd_evt_date  => pd_evt_date -- дата текущего события.
                , pm_ovd_sum => m_ovd_ondate -- сумма просрочки, которую необходимо учесть при расчете
                -------
                , pb_isdbms => pb_isdbms -- Vct 03.06.2019
                , bWrite_to_grpLog => bWrite_to_grpLog -- Vct 03.06.2019
                -------
                , pm_sum_after_mn => m_dpr_after_curent
                , pa_potok_xnpv => a_potok_xnpv  -- текущий поток для оценки амортизации после потока
                -- Vct 28.08.2019 - признак добавления текущей даты (pd_evt_date) в поток
                , pb_flow_modified => b_flow_modified
                -- Vct 23.09.2019
                , i_evtdate_out_pos => i_evtdate_pos -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
                );
      ---------------------------
      -- если был слом графика, оперделяем сумму после потока по старому графику
      -- b_broken_schedule := ( d_modification_start = pd_evt_date );

-- Vct 25/07/2019 - снимаем условие на слом графика
--      IF b_broken_schedule
--        THEN -- был слом графика
           -- Vct 25.02.2019 встраиваем определение суммы будущих процентных корректировок
           -- по текущему (новому) графику определяем сумму будущих процентных корректировок
           -- a_potok_xnpv
        cd_fcc_pkg.bp_dpr_pending_revenue_diff(pa_potok => a_potok_xnpv -- входной поток
                                     , pn_rate => i_dpr_pecrent  -- ставка (амортизации), оценивающая поток
                                     , pd_reper_date => pd_evt_date -- опорная дата, по отношению к которой считается сумма будущих процентных корректировок
                                     -- Vct 05.03.2019 , Vct 28.08.2019 case z.195326
                                    -- , pn_Day_Shift => case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                    -- Vct 11.10.2019 z.196254 - всегда вперед
                                     , pn_Day_Shift => 1 -- case when b_flow_modified then 0 else  1 end -- сдвиг в днях по отношению к pd_reper_date для учитываемых корректировок (1 - день, 0 - включая pd_reper_date)
                                     -----------
                                     , pn_pending_diff => m_pending_diff -- полученная сумма будущих процентных корректировок с учетом знака.
                                     );

       /* -- rem Vct 26.02.2019
           -- получение суммы после потока по старому графику
        bp_XNPV_lc( pn_agrId => pn_agrId -- договор
                  -- Vct 14.02.2019
                  , pn_part => pn_part -- номер части,
                  , pc_dpr_code => vc_dpr_code --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
                  --
                  , pc_extflowsqlid => vc_extflowsqlid -- идентификатор точки входа для получения потока
                  , pc_odvclc_code  => vc_odvclc_code  -- кодировка способа вычисления даты постановки просрочки в поток
                  , pi_dpr_pecrent => i_dpr_pecrent -- ставка амортизации
                  , pd_flow_date => (d_modification_start - 1) -- дата, на которую следует получать поток
                  , pd_evt_date  => pd_evt_date -- дата текущего события.
                  , pm_ovd_sum => m_ovd_ondate -- сумма просрочки, которую необходимо учесть при расчете
                  , pm_sum_after_mn => m_dpr_after_prev
                  );
                  -- встраиваем определение суммы будущих процентных корректировок
      ELSE
        -- слома графика не было.
        -- сумму по предыдущему графику считаем равной текущей сумме после потока
        m_dpr_after_prev := m_dpr_after_curent;
       */
--      END IF;

  End;
  ----------------------------------------------------------------
  --------------------------------------------------------------
  -- формируем операцию по выведению сумм непроцентных корректировок (403/404)
  -- TODO !!! - проверка правильности положения сумм.
  Begin
    -- Vct 25.02.2019 -- сумму непроцентных корректировок выводим только если сегодня был слом графика
-- Vct 25/07/2019 - снимаем условие на слом графика
--    IF b_broken_schedule   -- Vct 26.02.2019
---      --Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN
      -------------------------------------------------------------------
    -- Vct 02.10.2019 z.195805 -- признак необходимости проведения непроцентной корректировки
    -- Vct 09.10.2019
    IF b_flow_modified
        And (Not Coalesce(pb_correction_required, False)) --сама  сумма клиентского платежа пока не используется иначе как для принятия решения о коррекции остатка непроц.
                                                          -- поэто не будем ее вычислять, если явно заказали выполнение коррекции
      THEN
        IF b_deprecation_by_part --cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN

          m_clipay := Get_CliPay_ByPart(p_AGRID =>  pn_agrId
                                      , p_ipart => pn_part  -- номер части
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

    b_need_pareCorrection :=  pb_correction_required -- заказали принудительно выводить остатки счета непроцентных корректировок
                       OR  b_broken_schedule -- сегодня сломан график
                       OR (b_flow_modified  -- клиентский платеж не дату планового графика, пока опознаем по признаку исходного отсутствия текущей даты в потоке
                          And m_clipay != cn_Zero -- m_fact_revenue != cn_Zero -- z.19805 - оценку непроцентных корректировок проводим только при наличии платежей клента на внеплановую дату
                          -- z.196758 в предшествующую дату по лановому графику не было выведения 520го счета...
                          -- (может быть придется и переформулировать это условие...)
                          And ( i_evtdate_pos > 1
                          -- rem Vct 11.12.2020 z.208543
                            --  And (d_prevevt_date < a_potok_xnpv(i_evtdate_pos - 1).ddate) -- !!! Vct 09.12.2020 -- найдена засада на договоре 22527/528 (просрочка, после которой амортизания вне графика и опять вне графика.
                              )
                          );


   IF b_need_pareCorrection
     THEN
        IF b_deprecation_by_part
        THEN
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pn_part =>  pn_part
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );
          -----
          /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- договор
                                               , pn_part => pn_part
                                              , pc_RT => 'T' -- тип расчета %%, для которого собираем сумму
                                                             -- пока ожидаем, что расчет прошел по T? дальше посмотрим
                                              , pd_onDate => pd_evt_date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                                              );
         */
        ELSE
         m_accrued_revenue :=  get_accrued_req(pn_agrID =>  pn_agrId -- идентификатор договора
                                            , pd_onDate => pd_evt_date  -- дата, на которую должны быть получены остатки
                                             );
         /*
         m_accrued_revenue := get_AccruedPercent(pn_agrId => pn_agrId -- договор
                                                , pc_RT => 'T' -- тип расчета %%, для которого собираем сумму
                                                               -- пока ожидаем, что расчет прошел по T? дальше посмотрим

                                                , pd_onDate => pd_evt_date -- текущая дата, ожидается соответствующей текущему расчетному интервалу
                                                );
        */
        END IF;

       -------------------
/*   -- rem Vct 11.10.2019 z.196254
     ----------------------------
     -- Vct z.195805 02.10.2019
     -- вычисляем сумму процентных корректировок в интервале между элементами планового потока
     IF b_flow_modified
       THEN
         -- определить интервал учета текущих процентных корректировок
       --  d_curctr_Start Date;  --| -- интервал времени, на котором требуется собрать сумму m_current_contribution
        IF i_evtdate_pos > 1 -- позиция выходного потока, соотвтествующая дате текущего события pd_evt_date
          THEN
           d_curctr_Start :=   a_potok_xnpv(i_evtdate_pos - 1).ddate;
           d_curctr_Start := d_curctr_Start + 1; -- следующий день от "предыдущего" элемента в потоке
        Else
           d_curctr_Start := cd_chdutils.cd_zero_cd_date; -- от начала веков
        END IF;
        ----
        d_curctr_End := pd_evt_date - 1;    -- предыдущий день от поданного на вход расчётного дня
        ----
        IF  b_deprecation_by_part
           THEN
           -- считаем по части
           m_current_contribution := get_cdeDprPrcSubSum_MfoSchm( pd_dateStart => d_curctr_Start -- начала временного интервала
                                                               , pd_dateEnd => d_curctr_End -- завершение интервала
                                                               , pn_agrId => pn_agrid -- идентификатор договора
                                                               , pn_part => pn_part -- номер части
                                                               );
         ELSE
           -- по договору в целом
           m_current_contribution := get_cdeDprPrcSubSum_MfoSchm( pd_dateStart => d_curctr_Start -- начала временного интервала
                                                               , pd_dateEnd => d_curctr_End -- завершение интервала
                                                               , pn_agrId => pn_agrid -- идентификатор договора
                                                               );
        END IF;
     END IF;
*/
     ----------------------------------------------
      -------------------------------------------------------------------
/* rem Vct 02.10.2019
      IF pb_isdbms OR bWrite_to_grpLog
        THEN
        vc_message := ' слом графика с изменением оценки потока m_dpr_after_curent:='||cd_utl2s.num_to_str_dot(m_dpr_after_curent)
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
      -- Непроцентная коректировка при смене графика.
      -------------------------------------------------------------------
      -- тут неясно, попробуем пока m_dpr_after_curent - m_dpr_after_prev
      -- n_evt_sum := m_dpr_after_curent - m_dpr_after_prev; -- rem Vct 26.02.2019

      ----------------------------------------------------------
      -- Vct 25.02.2019 меняем способ расчета непроцентной корректировки
      -- теперь будем считать как разницу между текущим непокрытым остатком составного из двух пар счета
      -- учета незакрытого остатка непроцентной корректировки и суммой будущих процентных корректировок

      -- m_pending_diff -- сумма будущих процентных корректировок
      -- если m_pending_diff отрицательна, то мы закрываем оставшийся доход, если положительна то расход

      -- m_revenue_diff Number; -- Vct 25.02.2019 - сумма процентной корректировки сегодняшнего дня с учетом знака
      -- m_pending_diff Number; -- Vct 25.02.2019 - полученная сумма будущих процентных корректировок на новом графике.
      -- m_pending_current_rest Number -- Vct 25.02.2019 - сумма остатка на предшествующую дату сложного счета учета дохода/расхода (непроцентной корректировки)

      -- сумма сложного счета в cdbal ((153-152)-(150-151))
      -- на предшествующую дату
      IF b_deprecation_by_part -- cd_dpr_utl.bf_DeprecateByPart_Code(vc_dpr_code)
        THEN
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue_p(pn_agrId => pn_agrID      -- договор
                                                                          , pn_part => pn_part -- часть
                                                                          , pd_ondate => pd_evt_date - 1 -- дата
                                                                           );
      ELSE
        m_pending_current_rest := cd_dpr_utl.get_dpr_pending_extrarevenue(pn_agrId => pn_agrID      -- договор
                                                                        , pd_ondate => pd_evt_date - 1 -- дата
                                                                         );
      END IF;

      -- отладка... TODO.. дописать условие вывода...
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

      -- добавляем сумму сегодняшних процентных корректировок
      m_pending_current_rest := m_pending_current_rest + m_revenue_diff;

     -- Vct 17.10.2019
     -- в сумме будущих корректировок учитываем доходы будущего периода
      m_pending_diff := m_pending_diff + m_accrued_revenue;
      ------------------
      /* rem Vct 11.10.2019 z.196254
      --------------------------------
      -- 05.09.2019 Vct - вычитаем процентную корректировку текущего дня из оценки будущих кооректировок
      -- для случая когда в текущий поток была принудительно вставлена ссегодняшняя строка
      -- (интерпретируем как отсутствие планового потока в текущем дне.)
      IF b_flow_modified
        THEN
        -- 02.10.2019 z.195805 здесь еще учесть сумму процентных корректировок
        m_pending_current_rest := m_pending_current_rest - m_revenue_diff - m_current_contribution;
        --------------------------------------------------------------------------
       --  m_pending_diff := m_pending_diff - m_revenue_diff; -- rem Vct
        ----
       -- db_out('cd_dpr.mk_current_dpr.3156', 'm_pending_diff='||m_pending_diff );
      END IF;
      */

      m_pending_evt_sum := cn_Zero;

      -- вычисляем сумму непроцентной корректировки
      IF    m_pending_current_rest > cn_Zero -- ранее был доход
        And m_pending_diff <= cn_Zero        -- далее тоже будет доход
        THEN -- предыдущая корректировка фиксировала доход и оставшаяся сумма тоже доходная
          m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          -----------------------------------------------------
      ELSIF m_pending_current_rest < cn_Zero -- ранее был расход
        And m_pending_diff >= cn_Zero         -- впереди тоже расход
        THEN -- предыдущая корректировка фиксировала расход и оставшаяся сумма тоже расходная
          -- m_pending_evt_sum := Abs(m_pending_diff) - Abs(m_pending_current_rest);
          m_pending_evt_sum := Abs(m_pending_current_rest) - Abs(m_pending_diff); -- z.192649
          -- sign(-1)
          -----------------------------------------------------
      ELSIF m_pending_current_rest >= cn_Zero -- ранее был доход
        And m_pending_diff > cn_Zero         -- впереди расход
        THEN -- меняется знак с дохода на расход
          m_pending_evt_sum := -1*(Abs(m_pending_diff) + Abs(m_pending_current_rest));

      ELSIF  m_pending_current_rest <= cn_Zero -- ранее был расход
         And m_pending_diff < cn_Zero        -- далее будет доход
         THEN
           m_pending_evt_sum := (Abs(m_pending_diff) + Abs(m_pending_current_rest));

      END IF;
      n_evt_sum := m_pending_evt_sum;

      i_evt_code := Case  -- Vct 25.12.2018 - замена действий в связи с уходом МФО учета на банковский                      --
                      When n_evt_sum > cn_Zero Then 401 --- 403 (мфо)
                      When n_evt_sum < cn_Zero Then 402 --404 (мфо)
                      Else ci_Zero
                    End;

/*
      i_evt_code := Case  -- Vct 25.12.2018 - замена действий в связи с уходом МФО учета на банковский
                    --  When n_evt_sum > cn_Zero  Then 401 --  401 --403 (мфо)
                    --  When n_evt_sum < cn_Zero  Then 402  --404 (мфо)
                      -- Vct 10.04.2019 что-то недопонятое с направлением проводок, разбираемся поменяв 401/402 местами (эксперимент)
                      When n_evt_sum > cn_Zero And sign(m_pending_current_rest) >= cn_Zero Then 401 --  401 --403 (мфо)
                      --When n_evt_sum > cn_Zero And sign(m_pending_current_rest) < cn_Zero Then 402 --  401 --403 (мфо)

                      When n_evt_sum < cn_Zero And sign(m_pending_current_rest) >= cn_Zero  Then 402  --404 (мфо)
                      --When n_evt_sum < cn_Zero Then 402 --  --404 (мфо)
                      Else ci_Zero
                    End;
 */
      n_evt_sum := Abs(n_evt_sum);

      IF i_evt_code != ci_Zero
        And n_evt_sum != cn_Zero
        THEN -- Vct 15.07.2019
          make_next_out_array_element(  icurPos => i_out_index
                                    , pa_evt_queue => pa_result
                                    , p_ncdeAgrid => pn_agrID      -- договор
                                    , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                    , p_icdeType => i_evt_code   -- тип события
                                    , p_dcdeDate => pd_evt_date -- дата события
                                    , p_mcdeSum => n_evt_sum
                                    , p_ccdeRem => 'Текущая непроцентная корректировка ам. по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                   -- Vct 15.10.2018 z. -- для 403/404 действие не должно быть декларативным
                                   , pc_Declarative => to_char(Null)
                                   );
      END IF;

--     END IF;  -- Vct 25/07/2019 - снимаем условие на слом графика
    END IF; -- Vct 02.10.2019 z.195805
  End;
  ------------------------------------------------------------------
  --------------------------------------------------------------
  -- формируем операцию по выведению остатка на 520 счете (событие 409 для положительных сумм, 410 - для отрицательных)
  Begin
    ------------------------------------------------------------
    --  i_evt_code cde.icdetype%Type;    -- код генерируемого события
    --  n_evt_sum Number;                -- временная переменная для хранения суммы события.


--    IF b_broken_schedule   -- Vct 26.02.2019
--      -- Abs(m_dpr_after_curent - m_dpr_after_prev) >= 0.01 -- rem Vct 26.02.2019
--      THEN -- !!!! здесь кривата что-то сделать!
      -- n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + (m_dpr_after_curent - m_dpr_after_prev); -- правая часть это сумма непроцентной корректировки
      -- Vct 26/02/2016
       n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent) + m_pending_evt_sum;
      -- Vct 05.10.2019
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);

--    ELSE
--      n_evt_sum :=  (n_dpr_full_money_prev - m_dpr_after_curent);
--    END IF;

    -- Vct 22.06.2018
    -- 520 оказался активным.
    -- исправляем корреспонденцию
    i_evt_code := Case
                    When n_evt_sum > cn_Zero Then 410 --409
                    When n_evt_sum < cn_Zero Then 409 --410
                    Else ci_Zero
                  End;
    n_evt_sum := Abs(n_evt_sum);
    -----------------------------
    IF pb_isdbms OR bWrite_to_grpLog
      THEN
      vc_message := ' коррект. остатка ам. (расчет) n_evt_sum:='||cd_utl2s.num_to_str_dot(n_evt_sum)
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
    -- корректировка суммы операции с учетом ранее поставленного в cde значения
    -- здесь теоретически может возникать отрицательное значение
    IF i_evt_code != ci_Zero
      THEN
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
                                       , pn_goalSum => n_evt_sum
                                       , pi_evt_goal => i_evt_code
                                       , pi_evt_anti => Case i_evt_code
                                                          When 409 Then 410
                                                          When 410 Then 409
                                                        End
                                        );
    ELSE
      -- здесь сумма операции n_evt_sum оказывается равной нулю, но в базу могло записаться ранее что-то другое...
      -- TODO - темное место, нужна доп. проверка...
      n_evt_sum2 := bf_mk_evt_difference(pn_agrID => pn_agrID -- код договора
                                       , pi_part => pn_part -- часть
                                       , pd_onDate => pd_evt_date -- дата, в которой происходит сравнение
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
      vc_message := ' коррект. остатка ам. (в базу:) n_evt_sum2:='||cd_utl2s.num_to_str_dot(n_evt_sum2)
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
    ----- формируем в выходном массиве строку для операции по 409/410му действию, 520 счет
    IF i_evt_code != ci_Zero
      And n_evt_sum2 != cn_Zero
      THEN
      make_next_out_array_element(  icurPos => i_out_index
                                  , pa_evt_queue => pa_result
                                  , p_ncdeAgrid => pn_agrID      -- договор
                                  , p_icdePart => pn_part --- !!! 1 ??? TODO - здесь непонятка  -- часть
                                  , p_icdeType => i_evt_code   -- тип события
                                  , p_dcdeDate => pd_evt_date -- дата события
                                  , p_mcdeSum => n_evt_sum2
                                  , p_ccdeRem => 'Корректировка текущего остатка суммы амортизации по договору '||cd_utl2s.num_to_str_dot(pn_agrID)  -- комментарий к операции  -- комментарий к операции
                                 );
    END IF;

  End;


  ---------------------------------------------------------
  -- Vct 01.02.2019 - добавляем перехват неожиданных ошибок z.190837
  -- с целью добавления информации о договоре/части/дате
  -- Имхо, это мог бы делать вызывающий...
 Exception
   WHEN OTHERS THEN
     ------
    Declare
      -- i_errorCode Number;
    Begin

      -- i_errorCode := SQLCODE; -- TODO - это можно было бы использовать для исключения модификации текста ошибки в каких-то "известных случаях"
                                 -- пока не используем...
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
          -- TODO: возможно, эта регистрация лишняя, Групповой процесс сам должен бы залогировать ошибку.
          CDGRP.LOG_PUT('E', pn_agrid, vc_message_text, vn_TypPrtkl, cc_LOGMSGTYPEID);
      END IF;
      -- заменяем код ошибки...
      raise_application_error(cd_errsupport.i_COMMON_ERROR, vc_message_text);
    End;

  ----------------------------------------------
 End;


 -------------------------------------------------------------------
  -- процедура для использования  в cdgrp.Recalc_CDD_Item302
 -- для реализации группового процесса '1051' - текущая амортизация
 -- специального кода для частей пока нет - по состоянию на 10.04.2018
 -- TODO - ввести доп. параметр, требующий или отказывающий в вычислении состояния.
 -- Vct 03.06.2019 - этот код остаётся для схемы микрофинансовых компаний,
 -- когда начальная амортизация ведется на паре внутримодульных счетов (153A/152P),
 -- а текущая на паре (151A/150P)
 -------------------------------------------
 /*
  Vct 15.04.2020 
  Проблема:
  Вызов этой процедры в зависимости от запускающей процедуры в cdgrp 
  может происходить более одного раза - для каждой части 
  -- это может приводить
  -- а) к повторному расчету состояния для каждой из частей
  -- б) (потенциально) к дублированию амортизационных операций для случая амортизации в целом по договору.
  ---
  для начальной амортизации приделывалась некая защита от дублирования операций
  ---
  Надо
  а) (может быть) использовать её для текущей амортизации
  б) добавить защиту от повторного расчёта состояния
 */
 Procedure mk_current_dpr( pn_agrId in Number -- договор
                         , pn_part in Integer -- часть - пока не используем...
                         , pd_evt_date in Date -- дата, в которой проводится операция
                         , pb_isdbms in Boolean -- признак вывода информации в dbms_output
                         , pa_result OUT NOCOPY cd_dpr.T_CDE_CALL_QUEUE_PLTAB -- Vct 31.10.2019 + nocopy
                         --- z.196126 Vct 10.10.2019
                         -- параметр обязательного выведения корректировки остатка сводного счета процентных/непроцентных корректировок
                         , pb_correction_required In Boolean Default False
                         ---
                        -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                         )
 Is
  vc_dpr_code cd_dpr_utl.T_DO_AMRTPROC; --  признак ведения амортизации по договору '0' - амрт не ведется '1' - ведется в целом по договору, 2 - ведется в разрезе частей
  ------------------------------------------
  vc_message cd_types.T_MAXSTRING;
  --vc_message_text cd_types.TErrorString;  -- для текста сообщений и неожиданных ошибок
  ------------------------------------------   
  cc_LOGMSGTYPEID constant cdop_gde.cdopgerrid%Type := 'AUTOGRP'; -- идентификатор "типа ошибки" для процедуры логирования
  vn_TypPrtkl   NUMBER := NULL; -- "тип протокола" для CDGRP.LOG_PUT, при Null устанавливается в пакетной переменной до текущего вызова.
  bWrite_to_grpLog Boolean := False; -- для групповых процессов использовать LOG_PUT
  ------------------------------------------  
  a_empty_result cd_dpr.T_CDE_CALL_QUEUE_PLTAB; -- пустой массив для явного указания в случае раннего возврата
 Begin
   
   -- проверка режима работы в автономном джобе и для такого случая отключим
   Check_DBMSOUT_Job_Mode();
   pa_result := a_empty_result; -- явная пустышка, формально лишняя, пусть будет для чистого выражения намерений 
   
   bWrite_to_grpLog := ( cdoper.get_ActivProcess Is Not Null);
  -------------------------------------------
  -------------------------------------------
   -- признак необходимости ведения амортизации ( с учетом системной настройки)
   vc_dpr_code := cd_dpr_utl.need_deprecation_by_part_C(pn_agrid => pn_agrid);
   
   IF cd_dpr_utl.bf_Not_Deprecate_Code(pc_amrtcode => vc_dpr_code)
     THEN -- ранний возврат, если не проставлен признак ведения амортизации на договоре
      vc_message:= 'по договору <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> амортизация не ведется (не выставлен признак ведения амортизации в условиях договора)';
     -- логируем для процесса
     IF bWrite_to_grpLog
       THEN -- амортизация для договора не ведется
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
       THEN -- установлен признак ведения амортизации по частям, но нет признака ведения амортизации на части
      vc_message:='договор <'|| cd_utl2s.num_to_str_dot(pn_agrId)
                         ||'> часть <'||pn_part
                         ||'> Не установлен признак вдения амортизации для части.';

     IF bWrite_to_grpLog
       THEN -- амортизация для части не ведется
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
   -- установка флага пересчёта состояния в глобальной переменной
   Setup_currentdpr_gchache(
                           pn_agrID => pn_agrid -- договор
                         , pc_dprcode => vc_dpr_code -- код вариант ведения амортизации: 0 - не ведется, 1 ведется на договоре, 2 ведется по частям
                         , pd_evt_date => pd_evt_date -- дата, в которой проводится операция   
                          );
   --------------------------  
   IF Not Calc520_From_RecordedIncome
     Then                     
     If isBankDeprecationSchema
       Then
       -- новая схема для банков
       mk_current_dpr_bnk( pn_agrId => pn_agrId -- договор
                         , pn_part => pn_part -- часть - пока не используем...
                         , pd_evt_date => pd_evt_date -- дата, в которой проводится операция
                         , pb_isdbms => pb_isdbms  -- признак вывода информации в dbms_output
                         , pa_result => pa_result
                         -- z.196126 10.10.2019
                         , pb_correction_required =>  pb_correction_required
                        -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                         --- Vct 15.05.2020
                         , pc_dpr_code => vc_dpr_code -- режим амортизации определяет вызывающий
                         , pb_Write_to_grpLog => bWrite_to_grpLog -- true - заполнять CDGRP.LOG_PUT(
                         );
     Else
       -- старая схема для MFO
       mk_current_dpr_inner( pn_agrId => pn_agrId -- договор
                           , pn_part => pn_part -- часть - пока не используем...
                           , pd_evt_date => pd_evt_date -- дата, в которой проводится операция
                           , pb_isdbms => pb_isdbms -- признак вывода информации в dbms_output
                           , pa_result => pa_result
                           -- z.196126 10.10.2019
                           , pb_correction_required =>  pb_correction_required
                           --- Vct 15.05.2020
                           , pc_dpr_code => vc_dpr_code -- режим амортизации определяет вызывающий                         
                           , pb_Write_to_grpLog => bWrite_to_grpLog -- true - заполнять CDGRP.LOG_PUT(
                           -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                           );
     End If;
   Else -- новый способ - расчет от учтенных амортизационных доходов
   If isBankDeprecationSchema
       Then
       -- схема для банков 
       mk_current_dpr_bnk_fv( pn_agrId => pn_agrId -- договор
                         , pn_part => pn_part -- часть - пока не используем...
                         , pd_evt_date => pd_evt_date -- дата, в которой проводится операция
                         , pb_isdbms => pb_isdbms  -- признак вывода информации в dbms_output
                         , pa_result => pa_result
                         -- z.196126 10.10.2019
                         , pb_correction_required =>  pb_correction_required
                        -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
                         --- Vct 15.05.2020
                         , pc_dpr_code => vc_dpr_code -- режим амортизации определяет вызывающий
                         , pb_Write_to_grpLog => bWrite_to_grpLog -- true - заполнять CDGRP.LOG_PUT(
                         );
       
     Else
       Null;
       RAISE_APPLICATION_ERROR(cd_errsupport.i_IGNORE_RESULT,'cd_dpr_current.mk_current_dpr: Вариант - схема МФО, остаток 520 т учтенного дохода не реализован.');
       /*
       -- схема для MFO
       mk_current_dpr_inner_fv( pn_agrId => pn_agrId -- договор
                           , pn_part => pn_part -- часть - пока не используем...
                           , pd_evt_date => pd_evt_date -- дата, в которой проводится операция
                           , pb_isdbms => pb_isdbms -- признак вывода информации в dbms_output
                           , pa_result => pa_result
                           -- z.196126 10.10.2019
                           , pb_correction_required =>  pb_correction_required
                           --- Vct 15.05.2020
                           , pc_dpr_code => vc_dpr_code -- режим амортизации определяет вызывающий                         
                           , pb_Write_to_grpLog => bWrite_to_grpLog -- true - заполнять CDGRP.LOG_PUT(
                           -- , pi_result OUT Pls_integer -- код ошибки; 0 - успех, 8192 - не ожидаемая ошибка
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
