
PROC SQL;
   CREATE TABLE WORK.BASE_USD AS 
   SELECT t1.ano_mes, 
          t1.cod_tp_emissao, 
          t1.cod_moeda, 
          /* SUM_of_RVR_ATUARIAL */
            (SUM(t1.RVR_ATUARIAL)) FORMAT=COMMAX20.2 AS SUM_of_RVR_ATUARIAL
      FROM WORK.PENDENTE_FNL t1
      GROUP BY t1.ano_mes,
               t1.cod_tp_emissao,
               t1.cod_moeda;
QUIT;


PROC EXPORT

DATA=BASE_USD
OUTFILE = "\\vbr001001-572\dados_contabil\Testes\Moeda_Estrangeira\MSG_RVR_USD_202602.xlsx"
DBMS=XLSX 
replace;

RUN;