CREATE OR REPLACE 
PACKAGE cd_avers IS
-- Подсистема Кредиты XXI. Групповая обработка договоров банка АВЕРС
Version     CONSTANT VARCHAR2 (250) := ' CD_AVERS Version = $id:{7.06.08} 2021.04.05 Pervov $';

-- MODIFICATION HISTORY
-- Person   Date        Version     Comments
-- -------- ------      ----------  ---------------------------------
-- ANT      2018.11.27 (7.04.00}    created
-- Pervov   2020.10.06 (7.04.02}    (206940)
-- Pervov   2020.10.13 (7.04.03}    Set_Prolong,Set_Prolong_RB (207066,207140)
-- ANT      2020.10.21 (7.04.04}    Set_PrcRate (207326, 207327)
-- Pervov   2020.10.23 (7.04.05}    make_subsidy (207240) Начисление субсидированной части процентов
-- ANT      2020.10.23 (7.04.06}    Check_RestLoanLocal (20 7414)
-- Pervov   2020.10.28 (7.04.07}    make_subsidy (207517)
-- ANT      2020.11.10 (7.04.08}    Set_PrcRate, Check_RestLoan (20 7806)
-- Pervov   2020.11.23 (7.04.09}    make_subsidy (207517) Номер пачки 1313
-- Pervov   2020.12.01 (7.04.10}    make_subsidy2 (208027)
-- Pervov   2020.12.18 (7.04.11}    make_subsidy2 (208027) --ошибка определения суммы
-- ANT      2021.01.18 (7.06.01}    Check_RestLoanLocal (21 0202)
-- ANT      2021.01.27 (7.06.02}    Check_RestLoanLocal (21 0202)
-- ANT      2021.01.28 (7.06.03}    Check_RestLoan (21 0495)
-- ANT      2021.02.01 (7.06.04}    ExecCloseAgrLocal... (21 0547)
-- ANT      2021.02.04 (7.06.05}    ExecCloseAgrLocal... (21 0547)
-- ANT      2021.02.15 (7.06.06}    Check_RestLoan (210899)
-- Pervov   2021.04.01 (7.06.07}    Get_Risk_rate_tbl, Set_Risk_rate (211794)
-- Pervov   2021.04.05 (7.06.08}    make_subsidy (212047)

---------------------------------------
cSessID      CONSTANT VARCHAR2(12) := USERENV('SESSIONID');

isDBMS         BOOLEAN := true;
--------------------------------------------------------------

--------------------------------------------------------------
-- Public function and procedure declarations
---------------------------------------------------------- Set_MinPay
-- Задание минимального платежа в последний день расчетного периода
-- В график гашения вставить гашение 5 процентов от c/задолженности
PROCEDURE  Set_MinPay(
    AGRID  NUMBER,
    ProcID NUMBER,
    PrcPay IN OUT NUMBER,--Ставка Мин Платежа
    PayMin IN OUT NUMBER -- "Не менее..."
    );
PROCEDURE Set_MinPayLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    );
---------------------------------------------------------- Set_PrcRate
-- Проверка соблюдения льготного периода в последний день платежного периода
PROCEDURE  Set_PrcRate(
    AGRID  NUMBER,
    ProcID NUMBER
    );
PROCEDURE Set_PrcRateLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    );

---------------------------------------------------------- Set_Prclong
--Пролонгация пластика (206940)
function Set_Prolong(pGrid IN number, pDate IN Date, cErr IN OUT varchar2) return number;
---------------------------------------------------------- Set_Prclong_RB
--Откат пролонгация пластика (207066) (Работает только в тот же день, что и пролонгация)
function Set_Prolong_RB(pGrid IN number, pDate IN Date, cErr IN OUT varchar2) return number;

--------------------------------------------------------- make_subsidy
--Начисление субсидированной части процентов
PROCEDURE  make_subsidy(
    ProcID NUMBER,
    pevDATE DATE,
    ACCD   VARCHAR2,
    pNameCB VARCHAR2
    );
--------------------------------------------------------- make_subsidy
--Начисление субсидированной части процентов
PROCEDURE  make_subsidy2(
    ProcID NUMBER,
    pevDATE DATE,
    ACCD   VARCHAR2,
    pNameCB VARCHAR2
    );

---------------------------------------------- Check_RestLoan
-- Контроль остатка и обнуление ставки при нулевом остатке  20 7414
PROCEDURE  Check_RestLoan(
    AGRID  NUMBER
    );
PROCEDURE Check_RestLoanLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    );
-------------------------------------------------------------
-- закрытие договоров
-- 1. Проверка договора на исполнение всех требований:
--                погашена срочная задолженность,
--                погашена просроченная задолженность,
--                уплачены все проценты,
--                уплачены пени и комиссии.
--                по линиям - в случае погашения всех требований И наступления даты окончания договора
-- В случае выполнения всех перечисленных выше условий, требуется далее:
--                списать все резервы (в случае наличия),
--                списать обеспечение (с прописанием соответствующего условия и в случае наличия),
--                перевести договор в статус "Завершенный",
--                закрыть все договорные счета, включая текущий (в случае отсутствия остатков на счете).
-- В протокол выводить информацию по закрытым договорам.
PROCEDURE ExecCloseAgr(
    AGRID  NUMBER,
    ProcID NUMBER,
    OpName VARCHAR2, -- мемо1 комбинированной операции по закрытию остатов дна договоре
    is_cls_acc NUMBER -- 1 - закрываем счета
    );
PROCEDURE ExecCloseAgrLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE,
    OpName     VARCHAR2, -- мемо1 комбинированной операции по закрытию остатов дна договоре
    is_cls_acc NUMBER -- 1 - закрываем счета
    );
---------------------------------------------------------
--Определение категории качества и процента резервирования в зависимости от коэфф. платежеспособности и длительности просрочки
PROCEDURE Get_Risk_rate_tbl(pLPLT in number, pPRSR in number,pKK out number, pPR out number, cERR out varchar2);
---------------------------------------------------------
--Задание категории качества и процента резервирования в зависимости от коэфф. платежеспособности в дату первого платежа по ОД
--для группового процесса 1696
PROCEDURE Set_Risk_rate(pDate date default cd.Get_LsDate);
---------------------------------------------------------
end CD_AVERS;
/

GRANT EXECUTE ON cd_avers TO odb
/
GRANT DEBUG ON cd_avers TO odb
/

CREATE OR REPLACE 
PACKAGE BODY cd_avers is
--
---------------------------------------------------------------------- DBMS_PUT
PROCEDURE DBMS_PUT(Messer IN VARCHAR2) IS
BEGIN
    dbms_output.put_line(Messer);
END DBMS_PUT;
---------------------------------------------------------- Set_MinPay
-- Задание минимального платежа в последний день расчетного периода
-- В график гашения вставить гашение 5 процентов от c/задолженности
--
PROCEDURE  Set_MinPay(
    AGRID  NUMBER,
    ProcID NUMBER,
    PrcPay IN OUT NUMBER,--Ставка Мин Платежа
    PayMin IN OUT NUMBER -- "Не менее..."
    )
IS
  LSdate  DATE  := CD.Get_LSDATE;

    EM     VARCHAR2(20):='OK';
    ErrMsg VARCHAR2(256);

    sm      NUMBER;
--    smClc   NUMBER;
    smLoan  NUMBER;
    smLPln  NUMBER;
--    smSum   NUMBER;
--    smPayS  NUMBER;
    smPayL  NUMBER;
    smPayL0 NUMBER;
    isReset NUMBER;

    dd      DATE;

    CURSOR CurCDA IS
        SELECT * FROM CDA
          WHERE nCDAagrid=AGRID;

BEGIN

--dbms_output.put_line('->');
   FOR RecCDA IN CurCDA LOOP
dbms_output.put_line('Set_MinPay :   RecCDA.nCDAagrid = '||RecCDA.nCDAagrid);
      begin
      FOR RecCDS IN (select * from cds s
            where s.ncdsagrid=RecCDA.nCDAagrid
--               and LSdate = dcdsintcalcdate
               and LSdate between CDPCALISO.next_workday$(null,dcdsintcalcdate,-1) and dcdsintcalcdate
            )
      LOOP

      PrcPay := NVL(ATTRIBUTE_PKG.Get_Value_N(RecCDA.Icdaextend_Id,100207,LSdate),PrcPay);
      PayMin := NVL(ATTRIBUTE_PKG.Get_Value_N(RecCDA.Icdaextend_Id,100187,LSdate),PayMin);

      smLoan := CDBALANCE.get_CurSaldo(RecCDA.NCDAAGRID,1,null,null,RecCDS.Dcdsintcalcdate);

dbms_output.put_line('   PrcPay = '||PrcPay);
dbms_output.put_line('   PayMin = '||PayMin);
dbms_output.put_line('   PayDay='||RecCDS.Dcdsintpmtdate);
dbms_output.put_line('   smLoan='||smLoan);
dbms_output.put_line('   dcdsintcalcdate='||RecCDS.dcdsintcalcdate);
dbms_output.put_line('   Dcdsintpmtdate='||RecCDS.Dcdsintpmtdate);

      smPayL := ROUND((smLoan*PrcPay/100),2);

dbms_output.put_line('    -> :   smPayL='||smPayL);
      smPayL := LEAST( GREATEST(smPayL,PayMin),smLoan) ;
dbms_output.put_line('    ->>:   smPayL='||smPayL);


           IF RecCDA.ICDALINETYPE in (0,1,2) THEN -- простые кредитные линии -> коррекция истории расчиcтки]
               -- Ограничиваем предыдущий интервал оплаты ОД
               begin
                  SELECT dcdcdate INTO dd FROM cdc WHERE ncdcagrid=RecCDA.NCDAAGRID and dcdcdate=LSdate;
               exception when no_data_found then
                  INSERT INTO cdc(ncdcagrid,dcdcdate,mcdcsum,icdctype)
                  VALUES(RecCDA.NCDAAGRID,LSdate,0,2);
               end;
               -- задаем мин платеж
               begin
                  SELECT dcdcdate INTO dd FROM cdc WHERE ncdcagrid=RecCDA.NCDAAGRID and dcdcdate=RecCDS.Dcdsintpmtdate;
                  UPDATE cdc SET
                     mcdcsum=smPayL
                  WHERE ncdcagrid=RecCDA.NCDAAGRID and dcdcdate=RecCDS.Dcdsintpmtdate;

               exception when no_data_found then
                  INSERT INTO cdc(ncdcagrid,dcdcdate,mcdcsum,icdctype)
                  VALUES(RecCDA.NCDAAGRID,RecCDS.Dcdsintpmtdate,smPayL,2);
               end;

               CDBALANCE.ReSet_CDRR_DOG(RecCDA.NCDAAGRID,FALSE);

               sm:=CDBALANCE.get_CurSaldo(RecCDA.NCDAAGRID,22);
               CDOPER.process_SaveMess(
                  to_char(smPayL)||'#'||to_char(smLoan)||'#'||to_char(RecCDS.Dcdsintpmtdate,'YYYY-MM-DD')||'#'||to_char(sm)||'#','X',RecCDA.NCDAAGRID, 'MIN_PAY'); -- 143352

           ELSE -- многотраншевые линии
              smPayL0 := smPayL;
              isReset := 0;

              FOR recCDR IN (select distinct ICDRPART from cdr where NCDRAGRID=RecCDA.NCDAAGRID and DCDRDATE>=RecCDS.Dcdsintcalcdate)
              LOOP
                  smLoan:=CDBALANCE.get_CurSaldo(RecCDA.NCDAAGRID,1,recCDR.ICDRPART,null,RecCDS.Dcdsintcalcdate);
                  smLPln:=CDREP.get_PPLAN(RecCDA.NCDAAGRID,recCDR.ICDRPART,RecCDS.Dcdsintcalcdate);
                  IF smLoan<smLPln THEN
                      EM:=CDTERMS.Create_New_PartOut(RecCDA.NCDAAGRID,recCDR.ICDRPART,(smLPln-smLoan),RecCDS.Dcdsintcalcdate,ErrMsg);
--                      CDBALANCE.ReSet_CDRR_DOG_FROM(RecCDA.NCDAAGRID, recCDR.ICDRPART,RecCDS.Dcdsintcalcdate,FALSE);
                      isReset := 1;

dbms_output.put_line(' Reset  : -> Part = '||recCDR.ICDRPART||' Saldo='||smLoan||' / '||smLPln);
dbms_output.put_line('        : -> smPay = '||(smLPln-smLoan)||' /'||RecCDS.Dcdsintcalcdate);
                  END IF;
              END LOOP;

              FOR recCBC IN (select * from v_cdr r where ncdragrid=RecCDA.NCDAAGRID and  dcdrdate>=CD.Get_LSDATE order by icdrpart)
              LOOP
                  sm:=LEAST(smPayL, recCBC.mcdrrest);
                  IF recCBC.Mcdrsum>0 THEN -- Защита от "пустышки"
                     EM:=CDTERMS.Create_New_PartOut(RecCDA.NCDAAGRID,recCBC.icdrpart,sm,RecCDS.Dcdsintpmtdate,ErrMsg);
--                  CDBALANCE.ReSet_CDRR_DOG_FROM(RecCDA.NCDAAGRID,recCBC.icdrpart,RecCDS.Dcdsintcalcdate,FALSE);
                     isReset := 1;

dbms_output.put_line('        : -> Part = '||recCBC.icdrpart||' Saldo='||recCBC.mcdrrest||' / '||recCBC.dcdrdate);
dbms_output.put_line('        : -> smPay = '||sm||' /'||RecCDS.Dcdsintpmtdate);
                  END IF;
                  smPayL :=smPayL-sm;
                  IF smPayL<=0 THEN exit; END IF;
              END LOOP;

              IF isReset=1 THEN
                CDBALANCE.ReSet_CDRR_DOG(RecCDA.NCDAAGRID);
              END IF;

              IF smPayL0>smPayL THEN
              CDGRP.LOG_PUT('I',RecCDA.NCDAAGRID,'Установлен мин платеж '||(smPayL0-smPayL),NULL,'PRODUCT');
              END IF;
           END IF;

      END LOOP;

      end;

   END LOOP;

END Set_MinPay;
-----------------------------------------------------------------
PROCEDURE Set_MinPayLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    )
IS
--  EM     VARCHAR2(200):='OK';
--  ErrMsg VARCHAR2(1000);
  pPrcPay NUMBER:=5;
  pPayMin NUMBER:=0;-- артефакт (а вдруг понадобится)

          CURSOR crCD1 IS
          select * from cd1_tmp a;

BEGIN
dbms_output.put_line('Set_MinPayLocal :   00');
    for rec1 in crCD1 loop

      Set_MinPay( AGRID    => rec1.ncd1agrid,
                  ProcID   => pprocID,
                  PrcPay   => pPrcPay,
                  PayMin   => pPayMin);

      delete from cd1_tmp where ncd1agrid=rec1.ncd1agrid;
      commit;

    end loop;
END Set_MinPayLocal;


---------------------------------------------------------- Set_PrcRate
-- Проверка соблюдения льготного периода в последний день платежного периода
--
PROCEDURE  Set_PrcRate(
    AGRID  NUMBER,
    ProcID NUMBER
    )
IS
  LSdate  DATE  := CD.Get_LSDATE;
  PrcPay  NUMBER:=23; --Повышенная Ставка

   EM     VARCHAR2(20):='OK';
--    ErrMsg VARCHAR2(256);

   DateFrom  DATE;
   DateUp    DATE;

   smLoan  NUMBER;
   smPayed NUMBER;
   crPC    NUMBER;

   CURSOR CurCDA IS
        SELECT * FROM CDA
          WHERE nCDAagrid=AGRID;

BEGIN
dbms_output.put_line('>> Set_PrcRate /'||AGRID||'/'||LSdate);

   FOR RecCDA IN CurCDA LOOP
dbms_output.put_line('  RecCDA.nCDAagrid = '||RecCDA.nCDAagrid);
      begin

      PrcPay:= NVL(ATTRIBUTE_PKG.Get_Value_N(RecCDA.Icdaextend_Id,100186,LSdate),PrcPay);

      EM:='OK';

      FOR RecCDS IN (select * from
             (select s.*,
                CDPCALISO.next_workday$(null,dcdsintpmtdate,-1) dPmtDateWork,
                LAG(s.dcdsintcalcdate) over (partition by s.ncdsagrid order by dcdsintcalcdate) dtFrom,
                LEAD(s.dcdsintpmtdate) over (partition by s.ncdsagrid order by dcdsintpmtdate) dtNxt
              from cds s
              where s.ncdsagrid=RecCDA.nCDAagrid
             )
            where LSdate between dPmtDateWork and dcdsintpmtdate
            )
      LOOP
dbms_output.put_line('=> RecCDS => '||RecCDS.Dcdsintpmtdate);

-- Условие льготного периода
--            smLoan := CDSTATE.Get_Debit_Credit_TO(RecCDA.NCDAAGRID,RecCDS.Dcdsintcalcdate);
--            select SUM(e.inp) into smPayed
--            from v_cde0 e
--            where e.dog=RecCDA.NCDAAGRID and e.dat between (RecCDS.Dcdsintcalcdate+1) and LSdate;

            DateFrom := TRUNC(LSdate,'MM')-1;
            smLoan := CDSTATE.Get_Debit_Credit_TO(RecCDA.NCDAAGRID,DateFrom);
            select SUM(e.inp) into smPayed
            from v_cde0 e
            where e.dog=RecCDA.NCDAAGRID and e.dat between (DateFrom+1) and LSdate;

dbms_output.put_line('=> Payed => '||smPayed||' / '||smLoan||'('||DateFrom||')');

            IF smLoan>NVL(smPayed,0) THEN
               -- проставить повышенную ставку по траншам предыдущего и текущего месяца
                  crPC := CDTERMS.Get_Term_Activ(AgrID, 1, 'INTRATE',LSdate);
                  IF NVL(crPC,0)=0 THEN

                     DateUp   := CDBALANCE.get_DateUp_Cur(AgrID,1,null,null,DateFrom);
                     DateFrom := TRUNC(Add_months(LSdate,-1),'MM');
                     DateFrom := Greatest(DateFrom, NVL(DateUp,DateFrom)); -- (20 7806)

                     delete from cdh h where h.ncdhagrid=AgrID and h.ccdhterm='INTRATE' and h.dcdhdate>DateFrom;
                     CDTERMS.update_history(AgrID,1,'INTRATE',DateFrom,NULL,PrcPay);

                     -- отсрочка срока оплаты на следующий интервал
                     UPDATE cds s SET
                        s.dcdsintpmtdate=RecCDS.Dtnxt
                     WHERE s.ncdsagrid=Agrid and s.dcdsintcalcdate=RecCDS.Dcdsintcalcdate;
                  END IF;

                  CDGRP.LOG_PUT('I',RecCDA.NCDAAGRID,'Нарушено условие льгот.периода -> '||PrcPay||'% c '||DateFrom,NULL,'PRODUCT');

            END IF;

      END LOOP;

      end;

  END LOOP;
END Set_PrcRate;
----------------------------------------------------------------- Set_PrcRateLocal
PROCEDURE Set_PrcRateLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    )
IS
--  EM     VARCHAR2(200):='OK';
--  ErrMsg VARCHAR2(1000);
--  pPrcPay NUMBER:=5;
--  pPayMin NUMBER:=1000;

          CURSOR crCD1 IS
          select * from cd1_tmp a;

BEGIN
dbms_output.put_line('Set_PrcRateLocal :   00');
    for rec1 in crCD1 loop

      Set_PrcRate( AGRID    => rec1.ncd1agrid,
                  ProcID   => pprocID);

      delete from cd1_tmp where ncd1agrid=rec1.ncd1agrid;
      commit;

    end loop;
END Set_PrcRateLocal;
---------------------------------------------------------- Set_Prolong
function Set_Prolong(pGrid IN number, pDate IN Date, cErr IN OUT varchar2) return number is
ret number := 1;
imda    number;
NMDAINT_I   number; -- Тип интервала
NMDADTT_I   number; -- Тип определения дня начисления
NMDADTN_I   number; -- День начисления
NMDADTP_I   number; -- Сдвиг оплаты (<0 рабочих дней)
NMDADTPM_I  number; -- Сдвиг оплаты (месяцы)
NMDAOWD_I   number; -- Флаг исключения выходных
NMDAOWDCR_I number; -- Флаг сдвига даты начисления
CMDADTP_ENDM_I number default 0; --Флаг Сдвиг оплаты от последнего дня месяца
idoc    number;
icountDoc   number;
begin
    SAVEPOINT START_PROL;
    select cda.icdamd2num into imda from cda where cda.ncdaagrid = pGrid;
    select  a.NMDAINT_I,a.NMDADTT_I,a.NMDADTN_I,a.NMDADTP_I,a.NMDADTPM_I, a.NMDAOWD_I, a.CMDADTP_ENDM_I
        into NMDAINT_I,NMDADTT_I,NMDADTN_I,NMDADTP_I,NMDADTPM_I,NMDAOWD_I,CMDADTP_ENDM_I from cd_mda a where a.IMDANUM = imda;
    cdterms.init_CDS( pGrid , -- номер договора
        NMDAINT_I   ,-- Тип интервала
        NMDADTT_I   ,-- Тип определения дня начисления
        NMDADTN_I   ,-- День начисления
        NMDADTP_I   ,-- Сдвиг оплаты (<0 рабочих дней)
        NMDADTPM_I  ,-- Сдвиг оплаты (месяцы)
        NMDAOWD_I   ,-- Флаг исключения выходных
        NMDAOWDCR_I ,-- Флаг сдвига даты начисления
        CMDADTP_ENDM_I,--Флаг Сдвиг оплаты от последнего дня месяца
        trunc(sysdate), -- дата начала перерасчета
        pDate + 90); -- дата окончания перерасчета

    CDTERMS.update_history(pGrid,1,'DEND', trunc(sysdate),NULL,NULL,NULL,pDate + 90);
    select count(1) into icountDoc from cdh_doc where cdh_doc.ncdhagrid = pGRID and ICDHTYPE = 2;
    idoc := CDTERMS2.Insert_CDH_DOC(pGrid,
                             2, --дополнительное соглашение
                             to_char(pGrid)||'/'||to_char(icountDoc + 1), --Номер документа
                             trunc(sysdate), --Дата пролонгации
                             2, --пролонгация
                             null, --комментарий
                             'ZZZZDZZZZZZZZZZ',--флаги
                             pDate
                             );
    return ret;
exception when others then
    dbms_put(SQLERRM(SQLCODE));
    cErr := SQLERRM(SQLCODE);
    ret := 0;
    ROLLBACK TO START_PROL;
    return ret;
end Set_Prolong;
---------------------------------------------------------- Set_Prolong
function Set_Prolong_RB(pGrid IN number, pDate IN Date, cErr IN OUT varchar2) return number is
ret number := 1;
n   number;
begin
    SAVEPOINT START_PROL;
    select count(1) into n from cdh where cdh.ncdhagrid = pGrid and cdh.ccdhterm = 'DEND' and to_date(CCDHCVAL,'dd.mm.rrrr') = pDate + 90 and trunc(DCDHEDIT) = trunc(sysdate);
    if n = 0 THEN
        ret := 0;
        cErr := 'В дату '||trunc(sysdate)||' пролонгация не выполнялась';
    else
        delete cdh where cdh.ncdhagrid = pGrid and cdh.ccdhterm = 'DEND' and to_date(CCDHCVAL,'dd.mm.rrrr') = pDate + 90 and trunc(DCDHEDIT) = trunc(sysdate);
        delete cdh_doc where NCDHAGRID = pGrid and trunc(DCDHCREATE) = trunc(sysdate) and ICDHTYPE = 2 and CCDHTPCHANGE like '%D%';
    end if;
    return ret;
exception when others then
    dbms_put(SQLERRM(SQLCODE));
    cErr := SQLERRM(SQLCODE);
    ret := 0;
    ROLLBACK TO START_PROL;
    return ret;
end Set_Prolong_RB;

--------------------------------------------------------- make_subsidy
--Начисление субсидированной части процентов
PROCEDURE  make_subsidy(ProcID NUMBER, pevDATE DATE, ACCD varchar2,pNameCB VARCHAR2) IS
 rRegOut      IDoc_Reg.T_RegOut;
 EM           VARCHAR2(2000);
 ErMs         VARCHAR2(2000);
 ACCC         VARCHAR2(20); --счет доходов по отделению
 sumTRN       number;  --размер субсидии (рублей)
 sumPRC       number;  --сумма процентов уплаченная в текущем месяце
 nPRC         number; --ставка процентов по договору на начало периода
 nPRCCB       number; --ставка ЦБ на начало периода
 idCB         number; --Идентификатор ставки ЦБ
 agrmnt       cda.ccdaagrmnt%type;
 dsign        cda.dcdasigndate%type;
 begin
    for Rec in (select * from CD1_tmp) loop
        select ccmaaccacc,ccdaagrmnt,dcdasigndate into ACCC,agrmnt,dsign
            from cmaacc,cda where ncmaaccmak = cda.icdatemplate and ncmaaccotd = icdabranch and ncmaacctpprm = 225 and ncdaagrid = rec.ncd1agrid;
        DBMS_PUT('Счет дохдов '||ACCC);
        select sum(mcdesum) into sumPRC from cde where ncdeagrid = rec.ncd1agrid and dcdedate between  trunc(pevDATE,'MM') and add_months(trunc(pevDATE,'MM'),1)-1
            and icdetype in (3,8,13,30,72,113,130,172,300,706,708,716,718,726,728);
        DBMS_PUT('Сумма уплаченных процентов '||sumPRC);
        nPRC := cdrep_util.get_intRate(rec.ncd1agrid, trunc(pevDATE,'MM'));
        nPRC := case
                --when nPRC < 5 then 5 --(212047)
                when nPRC < 6 then 6 else nPRC
                end;
        DBMS_PUT('Ставка процентов на начало периода '||nPRC);
        select id into idCB  from ir_group where cimport_name =pNameCB;
        nPRCCB := cdterms2.Get_FLTRate(idCB,trunc(pevDATE,'MM'));
        DBMS_PUT('Ставка ЦБ на '||trunc(pevDATE,'MM')||'-'||nPRC);
        if nPRC > 0 then
            sumTRN := round(sumPRC / nPRC * (nPRCCB + 4 - nPRC),2);
        else
            sumTRN := 0;
        end if;
        DBMS_PUT('Сумма проводки '||sumTRN);
        if sumTRN > 0 then
            ErMs := MO.Register( ErrorMsg       =>  EM,
                        DebitAcc       =>  ACCD,
                        DebitCur       =>  CDTERMS.get_acccur(ACCD),
                        CreditAcc      =>  ACCC,
                        CreditCur      =>  CDTERMS.get_acccur(ACCC),
                        DebitSum       =>  sumTRN,
                        CreditSum      =>  sumTRN,
                        OpType         =>  1,
                        RegDate        =>  pevDATE,
                        DocDate        =>  pevDATE,
                        DocNum         =>  idoc_util.get_nextautonum(ITOP=>1, DCURDAY=>pevDATE, ISOP=>null),
                        BatNum         =>  1313,
                        Purpose        =>  'НАЧИСЛЕНИЕ СУБСИДИРУЕМОЙ ЧАСТИ ПРОЦЕНТОВ, КД № '||agrmnt||' ОТ '||dsign||' за '||
                                            to_CHAR(pevDATE,'MONTH YYYY','nls_date_language=russian')
                  );
            CDGRP.LOG_PUT('I',rec.ncd1agrid,'НАЧИСЛЕНИЕ СУБСИДИРУЕМОЙ ЧАСТИ ПРОЦЕНТОВ '||sumTRN,2,'m_subsidy');
        end if;
        DELETE FROM CD1_tmp WHERE ncd1AGRID=Rec.ncd1AGRID;
    end loop;
 exception when others then
    dbms_put(SQLERRM(SQLCODE));
    CDGRP.LOG_PUT('E',null,agrmnt||' '||sqlerrm,2,'m_subsidy');
 end make_subsidy;

--------------------------------------------------------- make_subsidy
--Начисление субсидированной части процентов
PROCEDURE  make_subsidy2(ProcID NUMBER, pevDATE DATE, ACCD varchar2,pNameCB VARCHAR2) IS
 rRegOut      IDoc_Reg.T_RegOut;
 EM           VARCHAR2(2000);
 ErMs         VARCHAR2(2000);
 ACCC         VARCHAR2(20); --счет доходов по отделению
 sumTRN       number;  --размер субсидии (рублей)
 sumPRC       number;  --сумма процентов уплаченная в текущем месяце
 nPRC         number; --ставка процентов по договору на начало периода
 nPRC1        number;
 nPRCCB       number; --ставка ЦБ на начало периода
 idCB         number; --Идентификатор ставки ЦБ
 agrmnt       cda.ccdaagrmnt%type;
 dsign        cda.dcdasigndate%type;
 begin
    for Rec in (select * from CD1_tmp) loop
        select ccmaaccacc,ccdaagrmnt,dcdasigndate into ACCC,agrmnt,dsign
            from cmaacc,cda where ncmaaccmak = cda.icdatemplate and ncmaaccotd = icdabranch and ncmaacctpprm = 225 and ncdaagrid = rec.ncd1agrid;
        DBMS_PUT('Счет дохдов '||ACCC);
        select sum(mcdesum) into sumPRC from cde where ncdeagrid = rec.ncd1agrid and dcdedate between  trunc(pevDATE,'MM') and add_months(trunc(pevDATE,'MM'),1)-1
            and icdetype in (3,8,13,30,72,113,130,172,300,706,708,716,718,726,728);
        DBMS_PUT('Сумма уплаченных процентов '||sumPRC);
        nPRC := cdrep_util.get_intRate(rec.ncd1agrid, trunc(pevDATE,'MM'));
        nPRC1 := case when nPRC < 6.5 then 6.5 else nPRC end;
        DBMS_PUT('Ставка процентов на начало периода '||nPRC);
        select id into idCB  from ir_group where cimport_name =pNameCB;
        nPRCCB := cdterms2.Get_FLTRate(idCB,trunc(pevDATE,'MM'));
        DBMS_PUT('Ставка ЦБ на '||trunc(pevDATE,'MM')||'-'||nPRCCB);
        if nPRC > 0 then
            sumTRN := round(sumPRC / nPRC * (nPRCCB + 3 - nPRC1),2);
        else
            sumTRN := 0;
        end if;
        DBMS_PUT('Сумма проводки '||sumTRN);
        if sumTRN > 0 then
            ErMs := MO.Register( ErrorMsg       =>  EM,
                        DebitAcc       =>  ACCD,
                        DebitCur       =>  CDTERMS.get_acccur(ACCD),
                        CreditAcc      =>  ACCC,
                        CreditCur      =>  CDTERMS.get_acccur(ACCC),
                        DebitSum       =>  sumTRN,
                        CreditSum      =>  sumTRN,
                        OpType         =>  1,
                        RegDate        =>  pevDATE,
                        DocDate        =>  pevDATE,
                        DocNum         =>  idoc_util.get_nextautonum(ITOP=>1, DCURDAY=>pevDATE, ISOP=>null),
                        BatNum         =>  ProcID,
                        Purpose        =>  'НАЧИСЛЕНИЕ СУБСИДИРУЕМОЙ ЧАСТИ ПРОЦЕНТОВ, КД № '||agrmnt||' ОТ '||dsign||' за '||
                                            to_CHAR(pevDATE,'MONTH YYYY','nls_date_language=russian')||
                                            ' по программе "Ипотека с государственной поддержкой 2020"'
                  );
            CDGRP.LOG_PUT('I',rec.ncd1agrid,'НАЧИСЛЕНИЕ СУБСИДИРУЕМОЙ ЧАСТИ ПРОЦЕНТОВ '||sumTRN,2,'m_subsidy2');
        end if;
        DELETE FROM CD1_tmp WHERE ncd1AGRID=Rec.ncd1AGRID;
    end loop;
 exception when others then
    dbms_put(SQLERRM(SQLCODE));
    CDGRP.LOG_PUT('E',null,agrmnt||' '||sqlerrm,2,'m_subsidy');
 end make_subsidy2;


---------------------------------------------- Check_RestLoan
-- Контроль остатка и обнуление ставки при нулевом остатке  20 7414
PROCEDURE  Check_RestLoan(
    AGRID  NUMBER
    )
IS
--  PrcPay  NUMBER:=23; --Повышенная Ставка

  LSdate DATE  := CD.Get_LSDATE;
--  dtUp   DATE;

  PB       NUMBER;
  smLoan   NUMBER;
  smPayed  NUMBER;
  smPrc    NUMBER;

  dtPay    DATE;
  DateFrom DATE;
  DateEnd  DATE;

BEGIN
  Dbms_Output.put_line('Check_RestLoan >> '||AGRID);

   FOR rcCDA IN (select * from cda c where c.NCDAAGRID=AGRID)
   LOOP
     DateEnd := CDTERMS.Get_CurEndDate(AGRID);
     IF LSdate>DateEnd THEN continue; END IF;  -- 210899

     PB := NVL(CDTERMS.Get_Term_Activ(AGRID,1,'INTRATE'),0);

     Dbms_Output.put_line(' crPc >> '||PB);

     IF PB>0 THEN
            smLoan := CDSTATE.Get_Debit_Credit_TO(RcCDA.NCDAAGRID,LSdate);

            DateFrom := TRUNC(LSdate,'MM')-1;

            IF smLoan>0 THEN  -- 21 0495
                        smLoan := CDSTATE.Get_Debit_Credit_TO(RcCDA.NCDAAGRID,DateFrom);
                        select SUM(e.inp) into smPayed
                          from v_cde0 e
                            where e.dog=RcCDA.NCDAAGRID and e.dat between (DateFrom+1) and LSdate;
dbms_output.put_line('  => Payed => '||smPayed||' / '||smLoan||'('||DateFrom||')');
                        smLoan := smLoan- NVL(smPayed,0);
            END IF;
            Dbms_Output.put_line(' Rest L >> '||smLoan);


-- Задолженность по процентам
            CDINTEREST.Set_Tp_CALCULATE('TO_CUR');
            CD.Recalc_Fine (rcCDA.nCDAagrid,'AI', FALSE,FALSE);
            CDINTEREST.Set_Tp_CALCULATE('FULL');

            SELECT SUM(MCDITOTAL-MCDIPAYED), MAX(i.DCDIPMTDUE) INTO smPrc, dtPay
                  FROM v_cdi i
                  WHERE ncdiagrid=RcCDA.NCDAAGRID  AND ccdiRT='R' AND DCDITO<=LSdate;

            Dbms_Output.put_line(' smPrc >> '||smPrc||'  '||dtPay);
            Dbms_Output.put_line(' Rest L >> '||smLoan);

       IF smLoan<=0 and smPrc=0 THEN
          IF LSdate>dtPay THEN
               CDTERMS.update_history(AgrID,1,'INTRATE',LSdate+1,NULL,0);
          ELSE
               CDTERMS.update_history(AgrID,1,'INTRATE',TRUNC(LSdate,'MM'),NULL,0);
          END IF;
               CDGRP.LOG_PUT('I',AgrID,'Обнуление тек ставки ',NULL,'PRODUCT');
       END IF;

     END IF;
   END LOOP;

END Check_RestLoan;
----------------------------------------------------------------- Check_CashLoanLocal
PROCEDURE Check_RestLoanLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE
    )
IS
          CURSOR crCD1 IS
          select * from cd1_tmp a;

BEGIN
dbms_output.put_line('Check_RestLoanLocal :   00');
    for rec1 in crCD1 loop

      Check_RestLoan( AGRID    => rec1.ncd1agrid);

      delete from cd1_tmp where ncd1agrid=rec1.ncd1agrid;
      commit;

    end loop;
END Check_RestLoanLocal;
--------------------------------------------------------------- ExecCloseAgr
-- 19 6843
-- закрытие договоров
-- 1. Проверка договора на исполнение всех требований:
--                погашена срочная задолженность,
--                погашена просроченная задолженность,
--                уплачены все проценты,
--                уплачены пени и комиссии.
--                по линиям - в случае погашения всех требований И наступления даты окончания договора
-- В случае выполнения всех перечисленных выше условий, требуется далее:
--                списать все резервы (в случае наличия),
--                списать обеспечение (с прописанием соответствующего условия и в случае наличия),
--                перевести договор в статус "Завершенный",
--                закрыть все договорные счета, включая текущий (в случае отсутствия остатков на счете).
-- В протокол выводить информацию по закрытым договорам.

PROCEDURE ExecCloseAgr(
    AGRID  NUMBER,
    ProcID NUMBER,
    OpName VARCHAR2, -- мемо1 комбинированной операции по закрытию остатов дна договоре
    is_cls_acc NUMBER -- 1 - закрываем счета
) IS

  LSdate  DATE  := CD.Get_LSDATE;

  EM     VARCHAR2(20):='OK';
  Ret    NUMBER;

--  OpName   VARCHAR2(100);

  smLoan NUMBER;
  smDebt NUMBER;
  smCred NUMBER;

   CURSOR CurCDA IS
        SELECT * FROM CDA
          WHERE nCDAagrid=AGRID and ICDASTATUS=2;

   CURSOR CUR_Mes IS
        SELECT ccapMESSAGE Mes
         FROM CAP
         WHERE ccapSESSIONID=cSessID and CCAPLEVEL='E'
         ORDER BY icapID;

BEGIN
dbms_output.put_line('>> ExecCloseAgr /'||AGRID||'/'||LSdate);

   EM:='OK';

   FOR RecCDA IN CurCDA LOOP
dbms_output.put_line('  RecCDA.nCDAagrid = '||RecCDA.nCDAagrid);

      IF RecCDA.Icdaisline>0 AND CDTERMS.Get_CurEndDate(RecCDA.ncdaagrid)>LSdate THEN exit; END IF;

      begin
      SAVEPOINT START_CALC;
            smLoan := CDSTATE.Get_Debit_Credit_TO(RecCDA.NCDAAGRID);
dbms_output.put_line('  smLoan = '||smLoan);
            IF smLoan=0 THEN
--               Ret := CDREP_UTIL2.Calc_State(AGRID,0,0);
               CDFINE.Recalc_Fine(AgrID, 'AIS', FALSE, FALSE);
               select SUM(e.mcdesum) into smCred from cde e where e.ncdeagrid=AGRID and e.icdetype=1 and rownum<2;
dbms_output.put_line('  smCred = '||smCred);

               select SUM(v.DPAYSUM) into smDebt from v_cd_pay v where v.NPAYAGRID=AGRID;
dbms_output.put_line('  smDebt = '||smDebt);

               IF NVL(smCred,0)>0  and NVL(smDebt,0) = 0 THEN -- нет долгов на договоре

                   CDENV.ClearMess;
                   CDENV.Clear_Temp_Table(RecCDA.Ncdaagrid);

                   CDGRP.CombiExec2(
                      pprocID  => null,
                      pevDATE  => LSdate,
                      AgrID    => RecCDA.Ncdaagrid,
                      Memo1    => OpName
                      );

                   IF CDENV.Is_ErrMess=1 THEN
                      FOR CrMs IN CUR_Mes LOOP
                        CDGRP.LOG_PUT('E',RecCDA.Ncdaagrid,CrMs.Mes,NULL,'CLOSEAGR');
                        EM:='ERR';
                      END LOOP;

                   ELSE
                     -- сменяем статус
                     CDENV.ClearMess;

                     IF CDSTATE.Guess_And_Set_Status(RecCDA.Ncdaagrid)=3 THEN
                        IF is_cls_acc=1 THEN
                           IF cdState2.Close_Acc(RecCDA.Ncdaagrid) > 0 THEN
                              CDGRP.LOG_PUT('E',RecCDA.Ncdaagrid,'Не удалось закрыть счета',NULL,'CLOSEAGR');
                              EM:='ERR';
                           END IF;

                           IF CDSTATE.Get_Status_Err is not null OR EM='ERR'   -- 21 0547
                           THEN
                              UPDATE cda c SET c.ICDASTATUS=2, c.DCDACLOSED=null
                              WHERE c.NCDAAGRID=RecCDA.Ncdaagrid;
                              EM:='ERR';

                              FOR rcCAP in (select * from cap c where c.ccapsessionid=cSessID and c.ccaplevel='I' and c.ccaptop='ACC') LOOP
                                 CDGRP.LOG_PUT('I',RecCDA.Ncdaagrid,rcCAP.Ccapmessage,NULL,'CLOSEAGR');  -- 19 7525
                              END LOOP;

                              CDGRP.LOG_PUT('E',RecCDA.Ncdaagrid,'Ошибка смены статуса договора',NULL,'CLOSEAGR');
                           END IF;
                        END IF;
                     ELSE
                       CDGRP.LOG_PUT('E',RecCDA.Ncdaagrid,'Ошибка смены статуса договора',NULL,'CLOSEAGR');
                       EM:='ERR';
                     END IF;

                     IF CDENV.Is_ErrMess=1 THEN
                        FOR CrMs IN CUR_Mes LOOP
                          CDGRP.LOG_PUT('E',RecCDA.Ncdaagrid,CrMs.Mes,NULL,'CLOSEAGR');
                          EM:='ERR';
                        END LOOP;
                     END IF;

                   END IF;

                   IF EM='OK' THEN
                    CDGRP.LOG_PUT('I',AgrID,'Договор закрыт',NULL,'CLOSE');
                   END IF;

               ELSE
                  ROLLBACK TO START_CALC;
               END IF;
            END IF;
      end;
   END LOOP;

END ExecCloseAgr;

----------------------------------------------------------------- Set_PrcRateLocal2
PROCEDURE ExecCloseAgrLocal(
    pprocID    EOD_TRN.ieodid%TYPE,
    pevDATE   DATE,
    OpName     VARCHAR2, -- мемо1 комбинированной операции по закрытию остатов дна договоре
    is_cls_acc NUMBER -- 1 - закрываем счета
    )
IS
  EM     VARCHAR2(200):='OK';
  ErrMsg VARCHAR2(1000);
  pPrcPay NUMBER:=5;
  pPayMin NUMBER:=1000;

          CURSOR crCD1 IS
          select * from cd1_tmp a;

BEGIN
dbms_output.put_line('ExecCloseAgrLocal :   00');
    for rec1 in crCD1 loop

      ExecCloseAgr( AGRID  => rec1.ncd1agrid,
                  ProcID   => pprocID,
                  OpName   => OpName,
                  is_cls_acc => is_cls_acc);

      delete from cd1_tmp where ncd1agrid=rec1.ncd1agrid;
      commit;

    end loop;
END ExecCloseAgrLocal;

---------------------------------------------------------
--Определение категории качества и процента резервирования в зависимости от коэфф. платежеспособности и длительности просрочки
PROCEDURE Get_Risk_rate_tbl(pLPLT in number, pPRSR in number,pKK out number, pPR out number, cERR out varchar2) is
begin
    cERR := null;
        if pPRSR = 1 then --До 30 дней
            if pLPLT >= 0.4 then pKK := 1; pPR := 0;
            elsif pLPLT < 0.4 and pLPLT >= 0.2500 then pKK := 2; pPR := 1;
            elsif pLPLT < 0.2500 and pLPLT >= 0.1500 then pKK := 2; pPR := 5;
            elsif pLPLT < 0.1500 and pLPLT >= 0.1000 then pKK := 2; pPR := 20;
            elsif pLPLT < 0.1000 and pLPLT >= 0.0600 then pKK := 3; pPR := 21;
            elsif pLPLT < 0.0600 and pLPLT >= 0.0400 then pKK := 3; pPR := 30;
            elsif pLPLT < 0.0400  then pKK := 3; pPR := 50;
            else pKK := null; pPR := null;
            end if;
        elsif pPRSR = 2 then --До 60 дней
            if pLPLT >= 0.4 then pKK := 2; pPR := 1;
            elsif pLPLT < 0.4 and pLPLT >= 0.2500 then pKK := 3; pPR := 21;
            elsif pLPLT < 0.2500 and pLPLT >= 0.1500 then pKK := 3; pPR := 30;
            elsif pLPLT < 0.1500 and pLPLT >= 0.1000 then pKK := 3; pPR := 50;
            elsif pLPLT < 0.1000 and pLPLT >= 0.0600 then pKK := 4; pPR := 51;
            elsif pLPLT < 0.0600 and pLPLT >= 0.0400 then pKK := 4; pPR := 80;
            elsif pLPLT < 0.0400  then pKK := 4; pPR := 100;
            else pKK := null; pPR := null;
            end if;
        elsif pPRSR = 3 then --свыше 60 дней
            if pLPLT >= 0.4 then pKK := 3; pPR := 21;
            elsif pLPLT < 0.4 and pLPLT >= 0.2500 then pKK := 4; pPR := 51;
            elsif pLPLT < 0.2500 and pLPLT >= 0.1500 then pKK := 4; pPR := 80;
            elsif pLPLT < 0.1500 and pLPLT >= 0.1000 then pKK := 4; pPR := 100;
            elsif pLPLT < 0.1000 and pLPLT >= 0.0600 then pKK := 5; pPR := 100;
            elsif pLPLT < 0.0600 and pLPLT >= 0.0400 then pKK := 5; pPR := 100;
            elsif pLPLT < 0.0400  then pKK := 5; pPR := 100;
            else pKK := null; pPR := null;
            end if;
        else
            pKK := null; pPR := null;
      cERR := 'Не определена длительность просрочки!!!';
        end if;
exception when others then
    dbms_output.put_line('cd_avers.Get_Risk_rate_tbl ERROR!!! '||sqlerrm);
    cERR := 'cd_avers.Get_Risk_rate_tbl ERROR!!! '||sqlerrm;
    pKK := null; pPR := null;
end Get_Risk_rate_tbl;
----------------------------------------------------------
--Задание категории качества и процента резервирования в зависимости от коэфф. платежеспособности в дату первого платежа по ОД
--для группового процесса 1696
PROCEDURE Set_Risk_rate(pDate in date default cd.Get_LsDate) is
  ErrMsg  varchar2(2000);
  aGrid      NUMBER; --текущий договор
  n_KPLT     NUMBER; --коэф. платежеспособности
  n_FP       NUMBER; --финансовое положение
  pPRSR      NUMBER;
  mSUMPLAN   NUMBER; --Сумма планового платежа
  mSUMREST   NUMBER; --Сумма задолженности по 1-му плановому платежу
  dDATPLAN   DATE;   --Дата 1-го планового платежа
  pRISKGRP   NUMBER; --Текущая группа риска на договоре
  pRATERES   NUMBER; --Текущий % резервирования на договоре
  pRATERESMIN   NUMBER; --Текущий min % резервирования на договоре
  pRISKGRP_N    NUMBER; --Новая группа риска на договоре
  pRATERES_N    NUMBER; --Новый % резервирования на договоре
  CURSOR rcda is (select cda.*,cde.* from cda,cde --договора с первым возвратом
  where ncdaagrid = ncdeagrid and cde.icdetype = 2
  and not exists (select 1 from cde b where b.ncdeagrid = ncdaagrid and b.icdetype = cde.icdetype and b.dcdedate < cde.dcdedate)
  and cde.dcdedate = pDate);
begin
    --cd.set_Lsdate(trunc(sysdate));
    for r in rcda loop
      aGrid := r.ncdaagrid;
      n_KPLT := to_number(replace(mstr.get_num_list(attribute_pkg.Get_Value(r.ICDAEXTEND_ID,10159,pDate,1)),',','.'));
        n_FP := to_number(PCUSATTR.get_cli_atr(219,r.icdaclient,pDate,0,0));
      if n_KPLT is null then
        --dbms_output.put_line('Договор '||aGrid||' Не задан коэффициент платежеспособности');
        cdgrp.LOG_PUT('E',aGrid,' Не задан коэффициент платежеспособности',2,'Set_Risk_r');
        continue;
      end if;
      if n_FP is null then
        --dbms_output.put_line('Договор '||aGrid||' Не задано финансовое положение заемщика');
        cdgrp.LOG_PUT('E',aGrid,' Не задано финансовое положение заемщика',2,'Set_Risk_r');
        continue;
      end if;
      select min(dcdrdate) into dDATPLAN from v_cdr where ncdragrid = aGrid ; --дата 1-го планового платежа
      select mcdrsum,mcdrrest into mSUMPLAN, mSUMREST from v_cdr a1 where a1.ncdragrid = aGrid and a1.dcdrdate = dDATPLAN; --Плановый платеж из v_cdr
      cdreserve.bp_riskgrp_rateres_minrateres(aGrid,pDate,pRISKGRP,pRATERES,pRATERESMIN); --текушие значения
      dbms_output.put_line('Текущие pRISKGRP='||pRISKGRP||' pRATERES='||pRATERES);
      if r.dcdedate <= dDATPLAN and r.mcdesum = mSUMPLAN then --Платеж вовремя или ранее и в полном объеме
         dbms_output.put_line('Платеж вовремя или ранее и в полном объеме');
         pPRSR := 1;
      else
         dbms_output.put_line('!!!Платеж не вовремя или не в полном объеме');
         if n_FP = 1 then pPRSR := 1;
         elsif n_FP = 3 then pPRSR := 2;
         elsif n_FP = 5 then pPRSR := 3;
         end if;
      end if;
      Get_Risk_rate_tbl(n_KPLT, pPRSR, pRISKGRP_N, pRATERES_N, ErrMsg);
      dbms_output.put_line('Новые pRISKGRP='||pRISKGRP_N||' pRATERES='||pRATERES_N);
      if ErrMsg is null then
         CDTERMS.update_history(aGrid,1,'RISKGRP',pDate,NULL, null, pRISKGRP_N);
         CDTERMS.update_history(aGrid,1,'RATERES',pDate,NULL, pRATERES_N);
         dbms_output.put_line('848 Договор '||aGrid||' n_KPLT='||n_KPLT||' n_FP='||n_FP);
         cdgrp.LOG_PUT('I',aGrid,'RISKGRP->'||pRISKGRP_N||' RATERES->'||pRATERES_N,2,'Set_Risk_r');
      else
         dbms_output.put_line('851 Договор '||aGrid||ErrMsg);
         cdgrp.LOG_PUT('E',aGrid,substr(ErrMsg,1,100),2,'Set_Risk_r');
      end if;
    end loop;
exception when others then
  dbms_output.put_line('Ошибка '||SQLERRM);
  cdgrp.LOG_PUT('E',aGrid,'Ошибка '||SQLERRM,2,'Set_Risk_r');
end Set_Risk_rate;
----------------------------------------------------------

END;
/

