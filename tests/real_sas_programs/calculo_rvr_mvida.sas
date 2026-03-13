/* ========================================================================================= */
/* 						PROGRAMAÇÃO PARA CALCULAR RVR - MVIDA - DIRETO E ACEITO              */
/* ========================================================================================= */


/* ========== Variaveis a serem preenchidas ========== */ 

/*#################################################################################################################### */

/*COMEÇA AQUI*/
;
	%let diretorio = \\vbr001001-572\dados_contabil\00 BASE\02.MVIDA\01 Base_Confitec;
	%let diretorio2 = \\vbr001001-572\dados_contabil\00 BASE\02.MVIDA\00 Arquivos_sas;
	%let entcodigo = 05665;
	%let mesref = %sysfunc(mdy(02,28,2026));
;
	%let anomes = %eval(%sysfunc(year(&mesref))*100+%sysfunc(month(&mesref)));
;

/* DADOS PARA O ARQUIVO SAP */ 

	%let data = '28.02.2026'; 
	%let periodo = 02; 
	%let Ref = 'RVR 022026'; 
	%let empresa	= MVIDA; 

;

/* IMPORTA A BASE COM OS % DE RESSEGURO */ 
PROC IMPORT 
		DATAFILE = "&diretorio.\percentual_resseguro.xlsx"
		DBMS = xlsx
		OUT = percentual_resseguro
		REPLACE; 
		/*sheet="Percen_resse_ramo";*/ 
		sheet="Percen_resse_ramo_produto"; 
		GETNAMES= yes; 
RUN; 
Proc SQL;
	Create Table percentual_resseguro
	as Select Distinct
			t1.COD_RAMO_CONTABIL,
			t1.COD_PRODUTO,
			t1.Percen_Cessao_utilizado,
			t1.Percentual_SAS
	from percentual_resseguro t1; 
Quit;
;
/* IMPORTA A BASE COM OS GRUPOS DE CÁLCULOS DOS FATORES DA RVR */ 
PROC IMPORT 
		DATAFILE = "&diretorio.\Grupo_RVR.xlsx"
		DBMS = xlsx
		OUT = Grupo_RVR_&anomes
		REPLACE; 
		sheet="Planilha1"; 
		GETNAMES= yes; 
RUN; 
Proc SQL;
	Create Table Grupo_RVR
	as Select Distinct
			t1.grupo_ramo_contabil,
			t1.cod_ramo_contabil,
			t1.Grupo_RVR as Grupo_Calculo
	from Grupo_RVR_&anomes t1; 
Quit;
;
/* IMPORTA A BASE COM OS FATORES DE RVR POR GRUPO DE CÁLCULO E AGGING */ 
PROC IMPORT 
		DATAFILE = "&diretorio.\Fatores_RVR.xlsx"
		DBMS = xlsx
		OUT = Fatores_RVR_&anomes
		REPLACE; 
		sheet="Planilha1"; 
		GETNAMES= yes; 
RUN; 
proc sql;
create table WORK.Fatores_RVR as select
		t1.cod_seguradora_susep,
		t1.cod_tp_emissao,
		t1."Status Vigencia"n as Status_Vigencia,
		t1."Status Cliente"n as Status_Cliente,
		t1."Status Apolice"n as Status_Apolice,
		t1."Status Parcela"n as Status_Parcela,
		t1."Agging meses"n as Agging_meses,
		t1."ref. Agging"n as ref_Agging,
		t1.Grupo_Calculo,
		(max(t1.FATOR_ATUARIAL)) format=commax20.18 as FATOR_ATUARIAL 
	from WORK.Fatores_RVR_&anomes t1
	where t1.cod_seguradora_susep=6238 or (t1.cod_seguradora_susep=5665 and Grupo_Calculo="Pessoas")
	group by
		t1.cod_seguradora_susep,
		t1.cod_tp_emissao,
		t1."Status Vigencia"n,
		t1."Status Cliente"n,
		t1."Status Apolice"n,
		t1."Status Parcela"n,
		t1."Agging meses"n,
		t1."ref. Agging"n,
		t1.Grupo_Calculo
	;
quit;
;
data fator_RVR;
	set Fatores_RVR;
	if FATOR_ATUARIAL<0 then FATOR_ATUARIAL=0;
	if FATOR_ATUARIAL>1 then FATOR_ATUARIAL=1;
run;
;
/* importação da base analitica */ 
/* CARREGA ZIP PENDENTE INICIO */
	filename inzip ZIP "&diretorio.\&anomes._PENDENTE_ANALITICO_&entcodigo..zip";
	/* identify a temp folder in the WORK directory */
	filename xl "%sysfunc(getoption(work))/&anomes._PENDENTE_ANALITICO_&entcodigo..txt" ;
	/* hat tip: "data _null_" on SAS-L */
	data _null_;
		/* using member syntax here */
		infile inzip("&anomes._PENDENTE_ANALITICO_&entcodigo..txt")
		lrecl=256 recfm=F length=length eof=eof unbuf;
		file xl lrecl=256 recfm=N;
		input;
		put _infile_ $varying256. length;
		return;
		eof:
		stop;
	run;
	;	
	DATA "&entcodigo._pend_analit_&anomes"n; /*"&entcod._PEND_ANALIT_&periodoini"n (compress=yes);*/
	    LENGTH
        ano_mes            8
        cod_seguradora_susep   8
        cod_apolice        8
        num_endosso      $ 10
        num_certificado  $ 15
        cod_produto        8
        cod_agencia        8
        cod_tp_emissao   $ 1
        cod_moeda          8
        dt_inicio_vigencia   8
        dt_fim_vigencia    8
        dt_emissao_doc     8
        qtde_parcelas      8
        dt_vencimento      8
        dt_inicio_cobertura_parcela   8
        dt_fim_cobertura_parcela   8
        num_parcela        8
        grupo_ramo_contabil   8
        cod_ramo_contabil   8
        grupo_ramo_emitido   8
        cod_ramo_emitido   8
        val_cobranca       8
        val_iof            8
        val_custo_apolice   8
        val_desconto       8
        val_adic_fracionamento   8
        val_custo_inicial_contratacao   8
        val_comissao       8
        val_estipulante    8
        val_cobranca_cosseguro   8
        val_desconto_cosseguro   8
        val_adic_fracionamento_cosseguro   8
        val_comissao_cosseguro   8
        val_estipulante_cosseguro   8
        val_cobranca_resseguro   8
        val_comissao_resseguro   8
        val_direito_creditorio   8
        val_comissao_agenciamento   8
        val_remuneracao_representante   8
        dt_inicio_vigencia_ori   8
        dt_fim_vigencia_ori   8
        cod_sistema_origem $ 14
        cpf_cnpj_segurado   8
        num_proposta       8
        IDLG               $50
        numero_externo   $ 40
        val_comissao_VC    8
        val_estipulante_VC   8
        val_comissao_agenciamento_VC   8
        val_remuneracao_representante_VC   8
        val_cobranca_VC    8 
		LETRA_SISTEMA_ORIGEM				$ 4;
    FORMAT
        ano_mes          BEST6.
        cod_seguradora_susep BEST5.
        cod_apolice      BEST20.
        num_endosso      $CHAR10.
        num_certificado  $CHAR15.
        cod_produto      BEST5.
        cod_agencia      BEST5.
        cod_tp_emissao   $CHAR1.
        cod_moeda        BEST3.
        dt_inicio_vigencia DDMMYY10.
        dt_fim_vigencia  DDMMYY10.
        dt_emissao_doc   DDMMYY10.
        qtde_parcelas    BEST3.
        dt_vencimento    DDMMYY10.
        dt_inicio_cobertura_parcela DDMMYY10.
        dt_fim_cobertura_parcela DDMMYY10.
        num_parcela      BEST3.
        grupo_ramo_contabil BEST2.
        cod_ramo_contabil BEST2.
        grupo_ramo_emitido BEST2.
        cod_ramo_emitido BEST2.
        val_cobranca     BEST10.
        val_iof          BEST7.
        val_custo_apolice BEST4.
        val_desconto     BEST4.
        val_adic_fracionamento BEST4.
        val_custo_inicial_contratacao BEST4.
        val_comissao     BEST9.
        val_estipulante  BEST9.
        val_cobranca_cosseguro BEST7.
        val_desconto_cosseguro BEST4.
        val_adic_fracionamento_cosseguro BEST4.
        val_comissao_cosseguro BEST4.
        val_estipulante_cosseguro BEST4.
        val_cobranca_resseguro BEST7.
        val_comissao_resseguro BEST4.
        val_direito_creditorio BEST7.
        val_comissao_agenciamento BEST7.
        val_remuneracao_representante BEST6.
        dt_inicio_vigencia_ori DDMMYY10.
        dt_fim_vigencia_ori DDMMYY10.
        cod_sistema_origem $CHAR14.
        cpf_cnpj_segurado BEST14.
        num_proposta     BEST20.
        IDLG             $CHAR50.
        numero_externo   $CHAR40.
        val_comissao_VC  BEST4.
        val_estipulante_VC BEST4.
        val_comissao_agenciamento_VC BEST4.
        val_remuneracao_representante_VC BEST4.
        val_cobranca_VC  BEST4. 
		LETRA_SISTEMA_ORIGEM				$CHAR4.;
    INFORMAT
        ano_mes          BEST6.
        cod_seguradora_susep BEST5.
        cod_apolice      BEST20.
        num_endosso      $CHAR10.
        num_certificado  $CHAR15.
        cod_produto      BEST5.
        cod_agencia      BEST5.
        cod_tp_emissao   $CHAR1.
        cod_moeda        BEST3.
        dt_inicio_vigencia DDMMYY10.
        dt_fim_vigencia  DDMMYY10.
        dt_emissao_doc   DDMMYY10.
        qtde_parcelas    BEST3.
        dt_vencimento    DDMMYY10.
        dt_inicio_cobertura_parcela DDMMYY10.
        dt_fim_cobertura_parcela DDMMYY10.
        num_parcela      BEST3.
        grupo_ramo_contabil BEST2.
        cod_ramo_contabil BEST2.
        grupo_ramo_emitido BEST2.
        cod_ramo_emitido BEST2.
        val_cobranca     BEST10.
        val_iof          BEST7.
        val_custo_apolice BEST4.
        val_desconto     BEST4.
        val_adic_fracionamento BEST4.
        val_custo_inicial_contratacao BEST4.
        val_comissao     BEST9.
        val_estipulante  BEST9.
        val_cobranca_cosseguro BEST7.
        val_desconto_cosseguro BEST4.
        val_adic_fracionamento_cosseguro BEST4.
        val_comissao_cosseguro BEST4.
        val_estipulante_cosseguro BEST4.
        val_cobranca_resseguro BEST7.
        val_comissao_resseguro BEST4.
        val_direito_creditorio BEST7.
        val_comissao_agenciamento BEST7.
        val_remuneracao_representante BEST6.
        dt_inicio_vigencia_ori DDMMYY10.
        dt_fim_vigencia_ori DDMMYY10.
        cod_sistema_origem $CHAR14.
        cpf_cnpj_segurado BEST14.
        num_proposta     BEST20.
        IDLG             $CHAR50.
        numero_externo   $CHAR40.
        val_comissao_VC  BEST4.
        val_estipulante_VC BEST4.
        val_comissao_agenciamento_VC BEST4.
        val_remuneracao_representante_VC BEST4.
        val_cobranca_VC  BEST4. 
		LETRA_SISTEMA_ORIGEM				$CHAR4.;
	    INFILE "%sysfunc(getoption(work))/&anomes._PENDENTE_ANALITICO_&entcodigo..txt"
				FIRSTOBS= 2 
				LRECL=450
		        TERMSTR=CRLF
		        DLM=';'
		        MISSOVER
		        DSD ;
	INPUT
        ano_mes          : ?? BEST6.
        cod_seguradora_susep : ?? BEST5.
        cod_apolice      : ?? BEST20.
        num_endosso      : $CHAR10.
        num_certificado  : $CHAR15.
        cod_produto      : ?? BEST5.
        cod_agencia      : ?? BEST5.
        cod_tp_emissao   : $CHAR1.
        cod_moeda        : ?? BEST3.
        dt_inicio_vigencia : ?? DDMMYY10.
        dt_fim_vigencia  : ?? DDMMYY10.
        dt_emissao_doc   : ?? DDMMYY10.
        qtde_parcelas    : ?? BEST3.
        dt_vencimento    : ?? DDMMYY10.
        dt_inicio_cobertura_parcela : ?? DDMMYY10.
        dt_fim_cobertura_parcela : ?? DDMMYY10.
        num_parcela      : ?? BEST3.
        grupo_ramo_contabil : ?? BEST2.
        cod_ramo_contabil : ?? BEST2.
        grupo_ramo_emitido : ?? BEST2.
        cod_ramo_emitido : ?? BEST2.
        val_cobranca     : ?? COMMA10.
        val_iof          : ?? COMMA7.
        val_custo_apolice : ?? COMMA4.
        val_desconto     : ?? COMMA4.
        val_adic_fracionamento : ?? COMMA4.
        val_custo_inicial_contratacao : ?? COMMA4.
        val_comissao     : ?? COMMA9.
        val_estipulante  : ?? COMMA9.
        val_cobranca_cosseguro : ?? COMMA7.
        val_desconto_cosseguro : ?? COMMA4.
        val_adic_fracionamento_cosseguro : ?? COMMA4.
        val_comissao_cosseguro : ?? COMMA4.
        val_estipulante_cosseguro : ?? COMMA4.
        val_cobranca_resseguro : ?? COMMA7.
        val_comissao_resseguro : ?? COMMA4.
        val_direito_creditorio : ?? COMMA7.
        val_comissao_agenciamento : ?? COMMA7.
        val_remuneracao_representante : ?? COMMA6.
        dt_inicio_vigencia_ori : ?? DDMMYY10.
        dt_fim_vigencia_ori : ?? DDMMYY10.
        cod_sistema_origem : $CHAR14.
        cpf_cnpj_segurado : ?? BEST14.
        num_proposta     : ?? BEST20.
        IDLG             : $CHAR50.
        numero_externo   : $CHAR40.
        val_comissao_VC  : ?? COMMA4.
        val_estipulante_VC : ?? COMMA4.
        val_comissao_agenciamento_VC : ?? COMMA4.
        val_remuneracao_representante_VC : ?? COMMA4.
        val_cobranca_VC  : ?? COMMA4. 
		LETRA_SISTEMA_ORIGEM				: ?? $CHAR4.;
		RUN;
;
/* DELETA PARCELAS 
	PROC SQL;

	DELETE 	 	
	FROM		"&entcodigo._pend_analit_&anomes"n
	WHERE
; 

	QUIT; */
;

/*#################################################################################################################### */

/* inicio do calculo */

data premio_pendente01; 
	set "&entcodigo._pend_analit_&anomes"n; 
	    FORMAT 	mes_ref 									ddmmyy10.
				INCONSIST 									$1.
				val_cobranca								commax20.2
	     		val_iof										commax20.2
	     		val_custo_apolice							commax20.2
	     		val_desconto								commax20.2
	     		val_adic_fracionamento                    	commax20.2
	     		val_custo_inicial_contratacao       		commax20.2
	     		val_comissao                              	commax20.2
	     		val_estipulante                      		commax20.2
	     		val_cobranca_cosseguro                    	commax20.2
	     		val_desconto_cosseguro                    	commax20.2
	     		val_adic_fracionamento_cosseguro     		commax20.2
	     		val_comissao_cosseguro                    	commax20.2
	     		val_estipulante_cosseguro            		commax20.2
	     		val_cobranca_resseguro                    	commax20.2
	     		val_comissao_resseguro                    	commax20.2
	     		val_direito_creditorio                    	commax20.2
	     		val_comissao_agenciamento            		commax20.2
	     		val_remuneracao_representante        		commax20.2
	     		base_ppng                                 	commax20.2
	     		dcd_base                                  	commax20.2
	     		calc_ppng_dec                             	commax20.2
	     		DIAS_VIGENTE                              	commax20.2
	     		DIAS_DECORRER                             	commax20.2
	     		CALC_DCD_DEC                              	commax20.2
	     		ESTIPULANTE_DEC                           	commax20.2
	     		AGENCIAMENTO_DEC                     		commax20.2
	     		REMUNERACAO_DEC                           	commax20.2
				fator_decorrer								commax20.18
	     		calc_ppng_dec_ori                          	commax20.2
	     		CALC_DCD_DEC_ori                           	commax20.2;
		 ATTRIB 	
					cod_apolice									FORMAT = $20.
	     			aging_dias_nome                      		FORMAT = $25. /*aumenta o tamanho da string*/ 
	     			venc_vinc                               	FORMAT = $25. 
	     			decorrer_e_decorrido                 		FORMAT = $25.;

		mes_ref = &mesref;  
	    DIAS = (dt_vencimento - mes_ref); 
	    DIAS_VIGENTE = dt_fim_vigencia - dt_inicio_vigencia + 1; 
		BASE_PPNG = (val_cobranca - val_cobranca_cosseguro - val_iof - val_adic_fracionamento - val_adic_fracionamento_cosseguro - val_custo_apolice); 
		DCD_BASE  = (val_comissao - val_comissao_cosseguro); 

		* VENCER_VENCIDO; 
		IF dt_vencimento < mes_ref
				THEN VENC_VINC = "Vencido";
			ELSE IF dt_vencimento >= mes_ref
				THEN VENC_VINC = "A Vencer"; 
		ELSE VENC_VINC = 0; 

	   * DECORRIDO E A DECORRER;
	   IF         DT_FIM_VIGENCIA < mes_ref       
	           then DECORRER_E_DECORRIDO = "Decorrido";
	     	ELSE IF   DT_FIM_VIGENCIA >= mes_ref 
	           then DECORRER_E_DECORRIDO = "Decorrer";   
	   ELSE DECORRER_E_DECORRIDO = 0; 

		*MARCA INCONSISTENTES - EXCLUÍDOS NO PROGRAMA ORIGINAL;
		IF		
							cod_agencia 				= 0
				OR			cod_produto 				= . 
				OR 		   	cod_produto 				= 0
				OR 			grupo_ramo_contabil		  	= . 
				OR 			grupo_ramo_contabil			= 0
				OR 			cod_ramo_contabil 			= . 
				OR 			cod_ramo_contabil 			= 0 
				OR			dt_inicio_vigencia 			= . 
				OR			dt_inicio_vigencia 			<= mdy(12,30,1900)
				OR			dt_fim_vigencia 			= . 
				OR			dt_fim_vigencia 			<= mdy(12,30,1900)
				OR			dt_vencimento		 		= . 
				OR			dt_vencimento 				<= mdy(12,30,1900)
				OR			cpf_cnpj_segurado 			= .
				OR 			cpf_cnpj_segurado 			= 0
				OR			dt_inicio_vigencia 			>= dt_fim_vigencia
			THEN INCONSIST = "S";
		ELSE INCONSIST = "N";

		* DIAS VIGENTES; 
	   IF         DT_FIM_VIGENCIA < mes_ref
	   			THEN DIAS_DECORRER = 0; 
	   		ELSE IF         DT_FIM_VIGENCIA >= mes_ref
	   			THEN DIAS_DECORRER = DT_FIM_VIGENCIA - MES_REF + 1; 
	   ELSE DIAS_DECORRER = 0; 

		* FATOR DECORRER;
		IF	DIAS_DECORRER <= 0 OR DIAS_VIGENTE <= 0 OR INCONSIST="S"
	     		THEN fator_decorrer = 0;
			ELSE IF	DIAS_DECORRER > DIAS_VIGENTE
				THEN fator_decorrer = 1; 
		ELSE fator_decorrer = DIAS_DECORRER /DIAS_VIGENTE; 

	   	CALC_PPNG_DEC = BASE_PPNG * fator_decorrer; 
	   	CALC_DCD_DEC = DCD_BASE * fator_decorrer;
	   	ESTIPULANTE_DEC = (val_estipulante - val_estipulante_cosseguro) * fator_decorrer;
		AGENCIAMENTO_DEC = val_comissao_agenciamento * fator_decorrer;
		REMUNERACAO_DEC =val_remuneracao_representante * fator_decorrer;

	    /* CLASSIFICA  AGING LIST */    
		IF INCONSIST = "S" 
				THEN aging_dias_nome = "Nenhuma das anteriores";
			ELSE IF	dias >= 0  AND dias  <= 30 
	       		THEN aging_dias_nome = "AV.000 a 030 dias";
	    	ELSE IF         dias > 30  AND dias  <= 60 
	         	THEN aging_dias_nome = "av.031 a 060 dias";
	    	ELSE IF         dias > 60  AND dias  <= 120 
	           	THEN aging_dias_nome = "av.061 a 120 dias";
	    	ELSE IF         dias > 120  AND dias  <= 180
	     		THEN aging_dias_nome =     "av.121 a 180 dias";
	    	ELSE IF         dias > 180  AND dias  <= 365
	     		THEN aging_dias_nome =     "av.181 a 365 dias";
	    	ELSE IF         dias > 365 
	     		THEN aging_dias_nome =     "av.acima de 365 dias"; 
	    	ELSE IF         dias < 0  AND dias  >= -30
	     		THEN aging_dias_nome =     "vc.001 a 030 dias venc"; 
	    	ELSE IF         dias < -30  AND dias  >= -60
	     		THEN aging_dias_nome =     "vc.031 a 060 dias venc"; 
	    	ELSE IF         dias < -60  AND dias  >= -120
	     		THEN aging_dias_nome =     "vc.061 a 120 dias venc";
	    	ELSE IF         dias < -120  AND dias  >= -180
	     		THEN aging_dias_nome =     "vc.121 a 180 dias venc";
	    	ELSE IF         dias < -180  AND dias  >= -365
	     		THEN aging_dias_nome =     "vc.181 a 365 dias venc";
	    	ELSE IF         dias < -365
	     		THEN aging_dias_nome =     "vc.acima de 365 dias venc"; 
	   ELSE aging_dias_nome = "Nenhuma das anteriores"; 


	     /* CALC_PPNG_DEC_ORI */

	     IF DIAS_DECORRER <= 0 OR DIAS_VIGENTE <= 0
	                THEN CALC_PPNG_DEC_ORI = 0; 
	     ELSE IF (BASE_PPNG / DIAS_VIGENTE * DIAS_DECORRER) > BASE_PPNG
	                then CALC_PPNG_DEC_ORI = BASE_PPNG ;
	                ELSE CALC_PPNG_DEC_ORI = (BASE_PPNG / DIAS_VIGENTE * DIAS_DECORRER); 

	     /* CALC_DCD_DEC_ORI */ 
	     
	     IF DIAS_DECORRER <= 0 OR DIAS_VIGENTE <= 0
	                THEN CALC_DCD_DEC_ORI = 0;
	     IF (DCD_BASE/DIAS_VIGENTE * DIAS_DECORRER) > DCD_BASE
	                then CALC_DCD_DEC_ORI = DCD_BASE;
					ELSE CALC_DCD_DEC_ORI = (DCD_BASE /DIAS_VIGENTE * DIAS_DECORRER); 


 run;
;
PROC SQL;
	CREATE TABLE premio_pendente_res 
		AS SELECT 	
			a.*, 
			b.percentual_sas
		FROM work.premio_pendente01 as a 
			LEFT JOIN work.percentual_resseguro as b
				ON	a.cod_ramo_contabil = b.cod_ramo_contabil
					AND	a.cod_produto = b.cod_produto;

QUIT; 
;
Data BASE_DIRETO;
	Set premio_pendente_res;*(where=(cod_tp_emissao = "D"));
	    FORMAT	premio_resseguro 	COMMAX20.4
				ppng_resseguro 		COMMAX20.2 
				DCD_RESSEGURO  		COMMAX20.2 
				Premio_Liquido 		COMMAX20.2
				RVR         		COMMAX20.2
				RVR_ORI       		COMMAX20.2;
		ATTRIB  CONDICOES 			FORMAT=$10.; 

		IF 	percentual_sas 	= . then percentual_sas = 0;

		premio_resseguro 	= ((val_cobranca - val_cobranca_cosseguro - val_iof) * percentual_sas);
	    premio_liquido 		= (val_cobranca - val_comissao - val_iof - val_cobranca_cosseguro + val_comissao_cosseguro - premio_resseguro + val_comissao_resseguro - val_estipulante);           
		PPNG_RESSEGURO 		= (CALC_PPNG_DEC * percentual_sas); 
		DCD_RESSEGURO 		= val_comissao_resseguro * fator_decorrer;
		RVR =(VAL_COBRANCA - VAL_IOF - VAL_ADIC_FRACIONAMENTO - VAL_COMISSAO - VAL_COBRANCA_COSSEGURO - VAL_ESTIPULANTE -val_adic_fracionamento_cosseguro + val_comissao_cosseguro + val_estipulante_cosseguro + val_comissao_resseguro - val_comissao_agenciamento - val_remuneracao_representante + AGENCIAMENTO_DEC + CALC_DCD_DEC - CALC_PPNG_DEC - DCD_RESSEGURO + ESTIPULANTE_DEC + PPNG_RESSEGURO - PREMIO_RESSEGURO + REMUNERACAO_DEC);
		
		IF (DECORRER_E_DECORRIDO = "Decorrido" AND VENC_VINC = "Vencido")
	        	THEN CONDICOES = "CONDICAO_1";
	    	ELSE IF (DECORRER_E_DECORRIDO = "Decorrido" AND VENC_VINC = "A Vencer")
	            THEN CONDICOES = "CONDICAO_2" ; 
	    	ELSE IF (DECORRER_E_DECORRIDO = "Decorrer" AND VENC_VINC = "Vencido" AND dias < 0 )
	            THEN CONDICOES = "CONDICAO_3" ;
	    	ELSE IF (DECORRER_E_DECORRIDO = "Decorrer" AND VENC_VINC = "A Vencer")
	            THEN CONDICOES = "CONDICAO_4" ;
		ELSE CONDICOES = "CONDICAO_F"; 

		RVR_ORI =(VAL_COBRANCA - VAL_IOF - VAL_ADIC_FRACIONAMENTO - VAL_COMISSAO - VAL_COBRANCA_COSSEGURO - VAL_ESTIPULANTE -val_adic_fracionamento_cosseguro + val_comissao_cosseguro + val_estipulante_cosseguro + val_comissao_resseguro - val_comissao_agenciamento - val_remuneracao_representante + AGENCIAMENTO_DEC + CALC_DCD_DEC_ORI - CALC_PPNG_DEC_ORI - DCD_RESSEGURO + ESTIPULANTE_DEC + PPNG_RESSEGURO - PREMIO_RESSEGURO + REMUNERACAO_DEC);

Run;
;
PROC SQL ;
	CREATE TABLE CPF_CNPJt 
		AS SELECT 
			CPF_CNPJ_SEGURADO, 
			MIN(DIAS) AS DIAS_MIN
	FROM 		WORK.BASE_DIRETO
	WHERE CONDICOES IN ("CONDICAO_1", "CONDICAO_3") AND INCONSIST = "N"
	GROUP BY	CPF_CNPJ_SEGURADO; 
QUIT;
DATA CPF_CNPJ;
	SET CPF_CNPJt;
		FORMAT 
			CONDICOES_V2 		$10.
			AGING_DIAS_NOME_V2  $25.
			AGING_DIAS_NOME_V3	$25.;
	DIAS=DIAS_MIN;
	CONDICOES_V2 = "CONDICAO_5";
	* CLASSIFICA  AGING LIST;
	IF INCONSIST = "S"
			THEN AGING_DIAS_NOME_V2 =  	"Nenhuma das anteriores";
		ELSE IF dias >= 0  AND dias  <= 30 
			THEN AGING_DIAS_NOME_V2 = "AV.000 a 030 dias";
	    ELSE IF         dias > 30  AND dias  <= 60 
			THEN AGING_DIAS_NOME_V2 = "av.031 a 060 dias";
	    ELSE IF         dias > 60  AND dias  <= 120 
			THEN AGING_DIAS_NOME_V2 = "av.061 a 120 dias";
	    ELSE IF         dias > 120  AND dias  <= 180
	        THEN AGING_DIAS_NOME_V2 =     "av.121 a 180 dias";
	    ELSE IF         dias > 180  AND dias  <= 365
	        THEN AGING_DIAS_NOME_V2 =     "av.181 a 365 dias";
	    ELSE IF         dias > 365 
	        THEN AGING_DIAS_NOME_V2 =     "av.acima de 365 dias"; 
	    ELSE IF         dias < 0  AND dias  >= -30
	        THEN AGING_DIAS_NOME_V2 =     "vc.001 a 030 dias venc"; 
	    ELSE IF         dias < -30  AND dias  >= -60
	        THEN AGING_DIAS_NOME_V2 =     "vc.031 a 060 dias venc"; 
	    ELSE IF         dias < -60  AND dias  >= -120
	        THEN AGING_DIAS_NOME_V2 =     "vc.061 a 120 dias venc";
	    ELSE IF         dias < -120  AND dias  >= -180
	        THEN AGING_DIAS_NOME_V2 =     "vc.121 a 180 dias venc";
	    ELSE IF         dias < -180  AND dias  >= -365
	        THEN AGING_DIAS_NOME_V2 =     "vc.181 a 365 dias venc";
	    ELSE IF         dias < -365
	        THEN AGING_DIAS_NOME_V2 =     "vc.acima de 365 dias venc"; 
	ELSE AGING_DIAS_NOME_V2 =  	"Nenhuma das anteriores"; 
	AGING_DIAS_NOME_V3=AGING_DIAS_NOME_V2;
RUN;
DATA CONDICAO_2_4;
	SET BASE_DIRETO (WHERE=(CONDICOES notin ("CONDICAO_1", "CONDICAO_3") OR INCONSIST = "S"));
RUN;
PROC SQL;
	CREATE TABLE BASE_ARRASTO
	AS SELECT 	    
				a.*, 
				b.CONDICOES_V2, 
				b.AGING_DIAS_NOME_V2
		FROM WORK.CONDICAO_2_4 	AS A 
			LEFT JOIN work.CPF_CNPJ AS B
				ON B.CPF_CNPJ_SEGURADO = A.CPF_CNPJ_SEGURADO; 
QUIT;
DATA BASE_ARRASTO_FINAL; 
	set 
		BASE_ARRASTO
		BASE_DIRETO (WHERE=(CONDICOES IN ("CONDICAO_1", "CONDICAO_3") AND INCONSIST = "N")); 
*	format RVR         		COMMAX20.2; 
*   RVR =(VAL_COBRANCA - VAL_IOF - VAL_ADIC_FRACIONAMENTO - VAL_COMISSAO - VAL_COBRANCA_COSSEGURO - VAL_ESTIPULANTE -val_adic_fracionamento_cosseguro + val_comissao_cosseguro + val_estipulante_cosseguro + val_comissao_resseguro - val_comissao_agenciamento - val_remuneracao_representante + AGENCIAMENTO_DEC + CALC_DCD_DEC - CALC_PPNG_DEC - DCD_RESSEGURO + ESTIPULANTE_DEC + PPNG_RESSEGURO - PREMIO_RESSEGURO + REMUNERACAO_DEC); 

	attrib CONDICOES_V2 	FORMAT = $15.;

		IF INCONSIST = "S" THEN CONDICOES_V2="CONDICAO_F";
			ELSE IF CONDICOES in ("CONDICAO_1" "CONDICAO_3") THEN CONDICOES_V2=CONDICOES;
			ELSE IF CONDICOES_V2 ne "" THEN CONDICOES_V2=CONDICOES_V2; 
		ELSE CONDICOES_V2="CONDICAO_0";

		IF INCONSIST = "S" THEN AGING_DIAS_NOME_V2="Nenhuma das anteriores";
			ELSE IF	(CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias >= 0  AND dias  <= 30	THEN AGING_DIAS_NOME_V2 = "AV.000 a 030 dias";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias > 30  AND dias  <= 60 	THEN AGING_DIAS_NOME_V2 = "av.031 a 060 dias";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias > 60  AND dias  <= 120 	THEN AGING_DIAS_NOME_V2 = "av.061 a 120 dias";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias > 120 AND dias  <= 180	THEN AGING_DIAS_NOME_V2 = "av.121 a 180 dias";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias > 180 AND dias  <= 365	THEN AGING_DIAS_NOME_V2 = "av.181 a 365 dias";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias > 365 					THEN AGING_DIAS_NOME_V2 = "av.acima de 365 dias"; 
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < 0  AND dias  >= -30		THEN AGING_DIAS_NOME_V2 = "vc.001 a 030 dias venc"; 
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < -30  AND dias  >= -60	THEN AGING_DIAS_NOME_V2 = "vc.031 a 060 dias venc"; 
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < -60  AND dias  >= -120	THEN AGING_DIAS_NOME_V2 = "vc.061 a 120 dias venc";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < -120  AND dias  >= -180	THEN AGING_DIAS_NOME_V2 = "vc.121 a 180 dias venc";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < -180  AND dias  >= -365	THEN AGING_DIAS_NOME_V2 = "vc.181 a 365 dias venc";
	    	ELSE IF (CONDICOES_V2 in ("CONDICAO_1" "CONDICAO_3") OR AGING_DIAS_NOME_V2 eq "" ) AND dias < -365					THEN AGING_DIAS_NOME_V2 = "vc.acima de 365 dias venc"; 
			ELSE IF AGING_DIAS_NOME_V2 ne "" THEN AGING_DIAS_NOME_V2=AGING_DIAS_NOME_V2;
	    ELSE AGING_DIAS_NOME_V2 = "Nenhuma das anteriores"; 
RUN;
/*
Data RVR.BASE_ARRASTO_FINAL_MMMAAAA (compress=yes);
	set BASE_ARRASTO_FINAL;
run;
*/


/* INSERIR VARIÁVEIS DA NOVA METODOLOGIA */ 
*LIBNAME RVR '\\vbr001001-572\dados_MSF\REGIONAL\work\RVR';
*%let periodofim=%eval(&anofim * 100 + &mesfim);
*%let anoini=%sysfunc( ifn(&mesfim=1 , %eval(&anofim - 1) , &anofim) ) ;
*%let mesini=%sysfunc( ifn(&mesfim=1 , 12 , %eval(&mesfim-1)) );
*%let periodoini=%eval(&anoini * 100 + &mesini);
;
%let periodoini=&anomes;
%let anoini=%sysfunc(year(&mesref));
%let mesini=%sysfunc(month(&mesref));
%let anofim=%sysfunc( ifn(&mesini=12 , %eval(&anoini + 1) , &anoini) );
%let mesfim=%sysfunc( ifn(&mesini=12 , 1 , %eval(&mesini + 1)) );
%let periodofim=%eval(&anofim * 100 + &mesfim);
;
		data pendenteini;
			set BASE_ARRASTO_FINAL;
			format 
				COD_RAMO $4.;
			IF (grupo_ramo_contabil*100+cod_ramo_contabil)>999
				then COD_RAMO=grupo_ramo_contabil*100+cod_ramo_contabil;
				else COD_RAMO=cats("0",grupo_ramo_contabil*100+cod_ramo_contabil);
		run;

	/* - cria bases de classificação dos pendentes - inicio */
		*Contrato : i)VIGENTE: Decorrido e Não Decorrido || ii)Vencimento mais antigo;
		PROC SQL;
		   CREATE TABLE WORK.ApolVgnteAtraso AS 
		   SELECT t1.ano_mes, 
		          t1.cod_seguradora_susep,
				  t1.cod_tp_emissao, 
		          t1.cod_sistema_origem, 
		          t1.cod_produto, 
		          t1.cod_apolice, 
		          t1.cpf_cnpj_segurado, 
		          /* MAX_of_dt_fim_vigencia */
		            (MAX(t1.dt_fim_vigencia)) FORMAT=DDMMYY10. AS dt_fim_vigencia_MAX, 
		          /* ContratoVigente */
		            (IFC((MAX(t1.dt_fim_vigencia))<mdy(&mesfim,1,&anofim),'DECORRIDO','DECORRER')) FORMAT=$9. AS VIGENTE, 
		          /* MIN_of_dt_fim_vigencia */
		            (MIN(t1.dt_vencimento)) FORMAT=DDMMYY10. AS MIN_dat_vcto_Apol,
		          /* SUM_of_val_cobranca */
		            (SUM(t1.val_cobranca)) FORMAT=COMMAX16.2 AS val_cobranca_SUM
		      FROM pendenteini t1
		      GROUP BY t1.ano_mes,
		               t1.cod_seguradora_susep,
					   t1.cod_tp_emissao,
		               t1.cod_sistema_origem,
		               t1.cod_produto,
		               t1.cod_apolice,
		               t1.cpf_cnpj_segurado
			  HAVING calculated val_cobranca_SUM > 0;
		QUIT;
		*Maior Atraso do Cliente;
		PROC SQL;
		   CREATE TABLE WORK.AtrasoCliente AS 
		   SELECT t1.ano_mes, 
		          t1.cod_seguradora_susep,
		          t1.cpf_cnpj_segurado, 
		          /* MIN_of_dt_fim_vigencia */
		            (MIN(t1.MIN_dat_vcto_Apol)) FORMAT=DDMMYY10. AS MIN_dat_vcto_Sgrd
		      FROM WORK.ApolVgnteAtraso t1
		      GROUP BY t1.ano_mes,
		               t1.cod_seguradora_susep,
		               t1.cpf_cnpj_segurado;
		QUIT;
		*insere informacoes de atraso e vigentes na base pendente inicial;
		Proc SQL;
			Create Table work.pendenteini_2	as
				Select
					t1.*,
					t2.VIGENTE,
					t2.MIN_dat_vcto_Apol,
					t3.MIN_dat_vcto_Sgrd,
							(IFC(t1.dt_vencimento<mdy(&mesfim,1,&anofim),"INADIMPLENTE","ADIMPLENTE")) 
						FORMAT=$12.
					as StatusParcela,
					(IFN((year(t1.dt_vencimento)*100+month(t1.dt_vencimento))>(&anofim*100+&mesfim),-2,(&anoini*12+&mesini-year(t1.dt_vencimento)*12-month(t1.dt_vencimento)))) 
						FORMAT=COMMAX10.0 
					AS AggingVctoParcela, 
		            (IFC(t2.MIN_dat_vcto_Apol<mdy(&mesfim,1,&anofim),'INADIMPLENTE','ADIMPLENTE')) 
						FORMAT=$12. 
					AS StatusApolice,
					(IFN((year(t2.MIN_dat_vcto_Apol)*100+month(t2.MIN_dat_vcto_Apol))>(&anofim*100+&mesfim),-2,(&anoini*12+&mesini-year(t2.MIN_dat_vcto_Apol)*12-month(t2.MIN_dat_vcto_Apol)))) 
						FORMAT=COMMAX10.0 
					AS AggingVctoApolice, 
		            (IFC(t3.MIN_dat_vcto_Sgrd<mdy(&mesfim,1,&anofim),'INADIMPLENTE','ADIMPLENTE')) 
						FORMAT=$12. 
					AS StatusCliente,
					(IFN((year(t3.MIN_dat_vcto_Sgrd)*100+month(t3.MIN_dat_vcto_Sgrd))>(&anofim*100+&mesfim),-2,(&anoini*12+&mesini-year(t3.MIN_dat_vcto_Sgrd)*12-month(t3.MIN_dat_vcto_Sgrd)))) 
						FORMAT=COMMAX10.0 
					AS AggingVctoCliente  
				from work.pendenteini t1
				left join work.ApolVgnteAtraso t2 ON
					t1.ano_mes=t2.ano_mes AND 
					t1.cod_seguradora_susep=t2.cod_seguradora_susep AND 
					t1.cod_tp_emissao=t2.cod_tp_emissao AND 
					t1.cod_sistema_origem=t2.cod_sistema_origem AND 
					t1.cod_produto=t2.cod_produto AND 
					t1.cod_apolice=t2.cod_apolice AND 
					t1.cpf_cnpj_segurado=t2.cpf_cnpj_segurado
		        left join work.AtrasoCliente t3 ON
					t1.ano_mes=t3.ano_mes AND 
					t1.cod_seguradora_susep=t3.cod_seguradora_susep AND 
					t1.cpf_cnpj_segurado=t3.cpf_cnpj_segurado;
		Quit;
;
data work.pendenteini_3;
	set work.pendenteini_2;
	format
	  	RVR_atu_negativa $3. 
 		datcobpclini ddmmyy10.
		datcobpclfim ddmmyy10.
		fator_decorrer_atu commax20.16
		CALC_PPNG_DEC_atu commax20.2
		CALC_DCD_DEC_atu commax20.2
		ESTIPULANTE_DEC_atu commax20.2
		AGENCIAMENTO_DEC_atu commax20.2
		REMUNERACAO_DEC_atu	commax20.2
		PPNG_RESSEGURO_atu commax20.2
		DCD_RESSEGURO_atu commax20.2
		RVR_atu commax20.2
		Status_Vigencia $9.
		Status_Cliente $12.
		Status_Apolice $12.
		Status_Parcela $12.
		ref_Agging $12.
		Agging_meses 3.
		RVR_Atu_fnl commax20.2;

	If qtde_parcelas=0 OR (dt_fim_vigencia-dt_inicio_vigencia)<31 OR (cod_sistema_origem="VIDANOVA" and cod_produto in (5 , 278 , 770 , 997 , 998)) 
			then datcobpclini=dt_inicio_vigencia;
	Else datcobpclini=dt_inicio_vigencia+(dt_fim_vigencia-dt_inicio_vigencia)/qtde_parcelas*(num_parcela-1);
	If qtde_parcelas=0 OR (dt_fim_vigencia-dt_inicio_vigencia)<31 OR (cod_sistema_origem="VIDANOVA" and cod_produto in (5 , 278 , 770 , 997 , 998)) 
			then datcobpclfim=dt_fim_vigencia;
	Else datcobpclfim=dt_inicio_vigencia+(dt_fim_vigencia-dt_inicio_vigencia)/qtde_parcelas*num_parcela;

	fator_decorrer_atu=ifn( (datcobpclfim-datcobpclini)<=0 or datcobpclfim<=mes_ref , 0 , ifn( datcobpclini>=mes_ref , 1 ,(datcobpclfim-mes_ref)/(datcobpclfim-datcobpclini) ));

	CALC_PPNG_DEC_atu 		= BASE_PPNG * fator_decorrer_atu; 
	CALC_DCD_DEC_atu 		= DCD_BASE * fator_decorrer_atu;
	ESTIPULANTE_DEC_atu 	= (val_estipulante - val_estipulante_cosseguro) * fator_decorrer_atu;
	AGENCIAMENTO_DEC_atu 	= val_comissao_agenciamento * fator_decorrer_atu;
	REMUNERACAO_DEC_atu 	= val_remuneracao_representante * fator_decorrer_atu;
	PPNG_RESSEGURO_atu 	= (CALC_PPNG_DEC_atu * percentual_sas); 
	DCD_RESSEGURO_atu 		= val_comissao_resseguro * fator_decorrer_atu;
	RVR_atu =(VAL_COBRANCA - VAL_IOF - VAL_ADIC_FRACIONAMENTO - VAL_COMISSAO - VAL_COBRANCA_COSSEGURO - VAL_ESTIPULANTE -val_adic_fracionamento_cosseguro + val_comissao_cosseguro + val_estipulante_cosseguro + val_comissao_resseguro - val_comissao_agenciamento - val_remuneracao_representante + AGENCIAMENTO_DEC_atu + CALC_DCD_DEC_atu - CALC_PPNG_DEC_atu - DCD_RESSEGURO_atu + ESTIPULANTE_DEC_atu + PPNG_RESSEGURO_atu - PREMIO_RESSEGURO + REMUNERACAO_DEC_atu);
	RVR_atu_negativa=ifc(RVR_atu<0 , "SIM" , "NAO");

	Status_Vigencia=VIGENTE;
	Status_Parcela=StatusParcela;
	Status_Apolice=ifc(Status_Parcela="INADIMPLENTE" , Status_Parcela , StatusApolice);
	Status_Cliente=ifc(Status_Apolice="INADIMPLENTE" , Status_Apolice , StatusCliente);
	ref_Agging=ifc(Status_Apolice="INADIMPLENTE" and Status_Parcela="ADIMPLENTE" , "vcto apolice" , "vcto parcela");
	/* Agging_meses */
		if Status_Apolice="ADIMPLENTE" then Agging_meses=-1;
		/*vcto apolice*/
			if Status_Apolice="INADIMPLENTE" and Status_Parcela="ADIMPLENTE" then Agging_meses=ifn(AggingVctoApolice<0, 0 , ifn(AggingVctoApolice>13, 13 , AggingVctoApolice));
		/*vcto parcela*/
			if Status_Apolice="INADIMPLENTE" and Status_Parcela="INADIMPLENTE" then Agging_meses=ifn(AggingVctoParcela<0, 0 , ifn(AggingVctoParcela>13, 13 , AggingVctoParcela));

	RVR_Atu_fnl=ifn(INCONSIST="S" or RVR_atu_negativa="SIM" , 0 , RVR_atu);
run;
;
proc sql;
	create table work.pendente_4 as select
		t1.*,
		t2.Grupo_Calculo
	from work.pendenteini_3 t1
		left join work.Grupo_RVR t2 ON
			t1.grupo_ramo_contabil=t2.grupo_ramo_contabil  AND 
			t1.cod_ramo_contabil=t2.cod_ramo_contabil;
quit;	
;
data pendente_5;
	set pendente_4;
	if cod_seguradora_susep=5665 then Grupo_Calculo="Pessoas";
	if cod_seguradora_susep=6238 and cod_produto=216 then Grupo_Calculo="Localiza";
run;
;
proc sql;
	create table work.pendente_fnl as select
		t1.*,
		t2.FATOR_ATUARIAL,
		((t1.RVR_Atu_fnl)*(t2.FATOR_ATUARIAL)) FORMAT=COMMAX20.2 AS RVR_ATUARIAL
	from work.pendente_5 t1
		left join work.fator_RVR t2 ON
			t1.cod_seguradora_susep=t2.cod_seguradora_susep AND
			t1.cod_tp_emissao=t2.cod_tp_emissao AND
			t1.Status_Vigencia=t2.Status_Vigencia AND
			t1.Status_Cliente=t2.Status_Cliente AND
			t1.Status_Apolice=t2.Status_Apolice AND
			t1.Status_Parcela=t2.Status_Parcela AND
			t1.Agging_meses=t2.Agging_meses AND
			t1.ref_Agging=t2.ref_Agging AND
			t1.Grupo_Calculo=t2.Grupo_Calculo;
quit;	
;

	PROC SQL;

	CREATE TABLE pendente_sap AS

	SELECT 		*
	FROM 		work.pendente_fnl
	WHERE		INCONSIST <> "S" AND RVR_atu_negativa <> "SIM" AND RVR_Atu > 0 ; 

	QUIT; 
;

/*#################################################################################################################### */


/*Separação Base RVR Direto*/

	PROC SQL;

	CREATE TABLE pendente_sap_direto AS

	SELECT 		*
	FROM 		work.pendente_sap
	WHERE		cod_tp_emissao = "D"; 

	QUIT; 

/*DATA sashelp.RVR_CONTABIL_&anomes.*/


	DATA base_final;
		SET pendente_sap_direto;
		FORMAT		C1131900000 				COMMAX20.4; 
		FORMAT		C2125190000 				COMMAX20.4; 
		FORMAT		C2128900000 				COMMAX20.4; 
		FORMAT		C2122290000 				COMMAX20.4; 
		FORMAT		C2123190000 				COMMAX20.4; 
		FORMAT		C2123390000 				COMMAX20.4; 
		FORMAT		C2123590000 				COMMAX20.4; 
		FORMAT		C1132900002					COMMAX20.4; 


	/* CONTA 1131900000 - */ 	
	C1131900000 = FATOR_ATUARIAL * (VAL_COBRANCA - CALC_PPNG_DEC_atu - VAL_IOF - VAL_ADIC_FRACIONAMENTO); 

	/* CONTA 1131900000 - */ 
	C2125190000 = FATOR_ATUARIAL * (VAL_COMISSAO - CALC_DCD_DEC_atu); 

	/* CONTA 1131900000 - */ 
	C2128900000 = FATOR_ATUARIAL * (VAL_COMISSAO_AGENCIAMENTO + val_remuneracao_representante + VAL_ESTIPULANTE - AGENCIAMENTO_DEC_atu - REMUNERACAO_DEC_atu - ESTIPULANTE_DEC_atu); 

	/* CONTA 1131900000 - */ 
	C2122290000 = FATOR_ATUARIAL * (VAL_COBRANCA_COSSEGURO - val_adic_fracionamento_cosseguro - val_estipulante_cosseguro); 

	/*CALCULA DOS VALORES DE RESSEGURO */ 

	C2123190000 = FATOR_ATUARIAL * (0.35 *(PREMIO_RESSEGURO - PPNG_RESSEGURO_atu + DCD_RESSEGURO_atu - VAL_COMISSAO_RESSEGURO)); 

	C2123390000 = FATOR_ATUARIAL * (0.19 *(PREMIO_RESSEGURO - PPNG_RESSEGURO_atu + DCD_RESSEGURO_atu - VAL_COMISSAO_RESSEGURO)); 

	C2123590000 = FATOR_ATUARIAL * (0.45 *(PREMIO_RESSEGURO - PPNG_RESSEGURO_atu + DCD_RESSEGURO_atu - VAL_COMISSAO_RESSEGURO)); 

	C1132900002 = FATOR_ATUARIAL * (VAL_COMISSAO_COSSEGURO);  

	
		ATTRIB SEGMENTO format = $15.; 
		attrib COD_SISTEMA_ORIGEM_TXT format=$2.; 

	     GRUPO_RAMO_TXT = put(GRUPO_RAMO_CONTABIL, z2.);
	    
	     COD_RAMO_TXT = put(COD_RAMO_CONTABIL, Z3.);

	 /*COD_PRODUTO_TXT = PUT(COD_PRODUTO, Z4.); --> ALTERADO */ 

	 COD_PRODUTO_TXT = SUBSTR(PUT(COD_PRODUTO, z5.),2,4); 

		 IF 	COD_SISTEMA_ORIGEM = "MVP"
		 		THEN COD_SISTEMA_ORIGEM_TXT = "T"; 
		ELSE IF COD_SISTEMA_ORIGEM = "SIES"
				THEN COD_SISTEMA_ORIGEM_TXT = "V"; 
		ELSE IF COD_SISTEMA_ORIGEM = "TRONADOR_MVIDA"
				THEN COD_SISTEMA_ORIGEM_TXT = "P";
		ELSE IF COD_SISTEMA_ORIGEM = "TRONADOR_MVI"
				THEN COD_SISTEMA_ORIGEM_TXT = "P"; 
		ELSE IF COD_SISTEMA_ORIGEM = "TRONADOR_MVIDA" 
				THEN COD_SISTEMA_ORIGEM_TXT = "E";
		ELSE IF COD_SISTEMA_ORIGEM = "VIDANOVA"
				THEN COD_SISTEMA_ORIGEM_TXT	= "J";
		ELSE IF COD_SISTEMA_ORIGEM = "CORPORATIVO"
				THEN COD_SISTEMA_ORIGEM_TXT	= "9"; 
		ELSE IF COD_SISTEMA_ORIGEM = "TRONWEB_MVID"
				THEN COD_SISTEMA_ORIGEM_TXT	= "X"; 
		ELSE IF COD_SISTEMA_ORIGEM = "TRONWEB_MVIDA"
				THEN COD_SISTEMA_ORIGEM_TXT = "X"; 
				/*ELSE COD_SISTEMA_ORIGEM_TXT = "DEU_RUIM"*/ ; 

		 SEGMENTO = cats(GRUPO_RAMO_TXT,COD_RAMO_TXT,COD_PRODUTO_TXT, COD_SISTEMA_ORIGEM_TXT); 
			
	   DROP  GRUPO_RAMO_TXT	COD_RAMO_TXT	COD_PRODUTO_TXT	 COD_SISTEMA_ORIGEM_TXT;


	RUN; 


	/* CRIA AS CONTAS CONTABEIS PARA ALOCAR OS VALORES DE RVR */ 

	 /* TRATAMENTO DA BASE COM AS CONTAS CONTABEIS */ 

	DATA base_sap; 
		SET			base_final; 
		FORMAT		C3152710000 			COMMAX20.2; 

	C3152710000 	=  	C1131900000 - C2125190000 -C2128900000 - C2122290000 -C2123190000 - C2123390000 - C2123590000 + C1132900002; 

	COD_AGENCIA_TXT = 	PUT(COD_AGENCIA, Z4.); 

	COD_AGENCIA02 	= 	CATS("C0020", COD_AGENCIA_TXT); 

	DROP COD_AGENCIA_TXT COD_AGENCIA; 

	KEEP 	CONDICOES_V2 grupo_ramo_contabil cod_ramo_contabil	COD_AGENCIA02 cod_produto cod_sistema_origem aging_dias_nome AGING_DIAS_NOME_V2	IDLG VAL_COBRANCA	VAL_IOF VAL_ADIC_FRACIONAMENTO	VAL_COMISSAO	VAL_ESTIPULANTE	VAL_COBRANCA_COSSEGURO	VAL_ADIC_FRACIONAMENTO_COSSEGURO	VAL_COMISSAO_COSSEGURO	VAL_ESTIPULANTE_COSSEGURO 
			VAL_COMISSAO_RESSEGURO	VAL_COMISSAO_AGENCIAMENTO	VAL_REMUNERACAO_REPRESENTANTE	AGENCIAMENTO_DEC	CALC_DCD_DEC	CALC_PPNG_DEC	DCD_RESSEGURO	ESTIPULANTE_DEC	PPNG_RESSEGURO	PREMIO_RESSEGURO	REMUNERACAO_DEC	RVR	RVR_ATUARIAL 
			FATOR_ATUARIAL	SEGMENTO C1131900000 C2125190000 C2128900000 C2122290000 C2123190000 C2123390000 C2123590000 C3152710000 C1132900002; 
	RUN; 

	/* SOMARIZA AS CONTAS CONTABEIS */ 
	PROC SQL; 

	CREATE TABLE  SUM_COND_1_3_5_03 AS 

	SELECT 		DISTINCT SEGMENTO, 
							COD_AGENCIA02, 
				SUM(C1131900000 ) AS TOT_1131900000 FORMAT COMMAX20.2, 
				SUM(C2125190000 ) AS TOT_2125190000 FORMAT COMMAX20.2, 
				SUM(C2128900000 ) AS TOT_2128900000 FORMAT COMMAX20.2, 
				SUM(C2122290000 ) AS TOT_2122290000 FORMAT COMMAX20.2, 
				SUM(C2123190000 ) AS TOT_2123190000 FORMAT COMMAX20.2, 
				SUM(C2123390000 ) AS TOT_2123390000 FORMAT COMMAX20.2, 
				SUM(C2123590000 ) AS TOT_2123590000 FORMAT COMMAX20.2, 
				SUM(C3152710000 ) AS TOT_3152710000 FORMAT COMMAX20.2, 
				SUM(C1132900002 ) AS TOT_1132900002 FORMAT COMMAX20.2

	FROM 		WORK.base_sap
	GROUP BY	SEGMENTO, COD_AGENCIA02
	ORDER BY 	SEGMENTO, COD_AGENCIA02; 

	QUIT; 

	 /* CRIA BASE CONTA: 1131900000*/ 

	PROC SQL;

	CREATE TABLE  sap01 AS 

	SELECT 		segmento , cod_agencia02 as centro_de_custo, TOT_1131900000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 
	/*INSERI O VALOR DA CONTA CONTABIL*/ 
	PROC SQL; 

	ALTER TABLE sap01

	ADD 	conta_contabil char(20); 		
	UPDATE 	work.sap01
	SET		conta_contabil = 	"1131900000"; 

	QUIT; 

	/* CRIA BASE CONTA: 2125190000*/ 
	PROC SQL;

	CREATE TABLE  sap02 AS 

	SELECT 		segmento , cod_agencia02 as centro_de_custo, TOT_2125190000  as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 

	PROC SQL; 

	ALTER TABLE sap02

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap02
	SET		conta_contabil = 	"2125190000"; 

	QUIT; 
	/* CRIA BASE CONTA: 2128900000*/ 

	PROC SQL;

	CREATE TABLE  sap03 AS 

	SELECT 		segmento , cod_agencia02 as centro_de_custo, TOT_2128900000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 

	PROC SQL; 

	ALTER TABLE sap03

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap03
	set		conta_contabil = 	"2128900000"; 

	QUIT; 
	/* CRIA BASE CONTA: 2122290000*/ 
	PROC SQL;

	CREATE TABLE  sap04 AS 

	SELECT 		segmento , cod_agencia02 as centro_de_custo, TOT_2122290000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 

	PROC SQL; 

	ALTER TABLE sap04

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap04
	SET 	conta_contabil = "2122290000"; 

	QUIT; 
	/* CRIA BASE CONTA: 2123190000*/ 
	PROC SQL;

	CREATE TABLE  sap05 AS 

	SELECT 			segmento , cod_agencia02 as centro_de_custo, TOT_2123190000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 
	PROC SQL; 

	ALTER TABLE sap05

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap05
	SET		conta_contabil = 	"2123190000"; 

	QUIT; 
	/* CRIA BASE CONTA: 2123390000 */ 
	PROC SQL;

	CREATE TABLE  sap06 AS 

	SELECT 		segmento , cod_agencia02 as centro_de_custo, TOT_2123390000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 
	PROC SQL; 

	ALTER TABLE sap06

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap06
	SET		conta_contabil = 	"2123390000"; 

	QUIT; 
	/* CRIA BASE CONTA: 2123590000 */ 
	PROC SQL;

	CREATE TABLE  sap07 AS 

	SELECT 		segmento, cod_agencia02 as centro_de_custo, TOT_2123590000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 

	PROC SQL; 

	ALTER TABLE sap07

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap07
	SET		conta_contabil = 	"2123590000"; 

	QUIT; 

	/*  ------- CRIA BASE CONTA: 3152710000 -------  ------- */ 

	PROC SQL;

	CREATE TABLE  sap08 AS 

	SELECT 		segmento, cod_agencia02 as centro_de_custo, TOT_3152710000 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 
	PROC SQL; 

	ALTER TABLE sap08

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap08
	SET		conta_contabil = 	"3152710000"; 

	QUIT; 

	PROC SQL;

	CREATE TABLE  sap09 AS 

	SELECT 		segmento, cod_agencia02 as centro_de_custo, TOT_1132900002 as montante 
	FROM 		work.SUM_COND_1_3_5_03 ; 

	QUIT; 
	PROC SQL; 

	ALTER TABLE sap09

	ADD 	conta_contabil char(20); 
	UPDATE 	work.sap09
	SET		conta_contabil = 	"1132900002"; 

	QUIT; 

	/*  ------- UNI AS TABELAS DE CONTAS CONTABEIS   ------- */

	PROC SQL; 

	CREATE TABLE uni_tabelas AS 

	SELECT 	*	FROM 	WORK.SAP01 UNION ALL
	SELECT 	*	FROM 	WORK.SAP02 UNION ALL
	SELECT 	*	FROM 	WORK.SAP03 UNION ALL
	SELECT 	*	FROM 	WORK.SAP04 UNION ALL
	SELECT 	*	FROM 	WORK.SAP05 UNION ALL
	SELECT 	*	FROM 	WORK.SAP06 UNION ALL
	SELECT 	*	FROM 	WORK.SAP07 UNION ALL
	SELECT 	*	FROM 	WORK.SAP08 UNION ALL
	SELECT 	*	FROM 	WORK.SAP09; 

	QUIT; 

	/*  ==================================================================================== */ 
	/*	 - APLICA A CHAVE DE LANÇAMENTO, DO TIPO: CREDITO E DEBITO.	
	/*	 - TRANSFORMAR OS VALORES NEGATIVOS EM POSITIVOS 
	/*	 - DELETA OS VALORES ZERADOS
	/*	==================================================================================== */

	DATA 	SAP;
		SET 	uni_tabelas;
		FORMAT  montante02 	commax20.2; 

	IF 			Conta_contabil = "1131900000" and montante < 0 
				THEN  		chave_de_lancamento = "D"; 
	ELSE IF  	Conta_contabil = "1131900000" and montante > 0		 	
				THEN 		chave_de_lancamento = "C"; 
	ELSE IF 	Conta_contabil = "1132900002" and montante > 0 
				THEN 		chave_de_lancamento = "C"; 
	ELSE IF 	Conta_contabil = "1132900002" and montante < 0
				THEN 		chave_de_lancamento = "D"; 
	ELSE IF 	Conta_contabil in( "2122290000",  "2123390000", "2123190000","2123590000",  "2125190000", "2128900000", "3152710000") and montante < 0
				THEN 		chave_de_lancamento = "C"; 
				ELSE 		chave_de_lancamento = "D"; 
	IF 			montante < 0 
				THEN 		montante02 = (montante * -1.00) ; 
				ELSE 		montante02 = montante; 
	IF 			montante = 0  THEN delete ; 

	RUN; 

	/* ------- INSERI OS CAMPOS DO ARQUIVO DO SAP  ------- */ 

	PROC SQL; 

	ALTER TABLE sap

	ADD Empresa 				  	 NUM(2), 
	data_de_lancamento				 CHAR(20), 
	Periodo 						 NUM(2),
	Data_do_Documento 				 char(20),
	Tipo_de_Documento 				 char(2),
	Fornecedor 						 char(2),
	Moeda							 char(3),
	Filial							 char(2),
	Centro_de_lucro					 char(2),
	Tipo_de_movimento			     char(2),
	Sociedade_Parceira 				 char(2),
	Referencia 						 char(10),
	Texto_Cabeçalho					 char(2),
	Texto_do_Item					 char(40),
	Usuario 						 char(2),
	tipo_de_conta					 char(2),	
	Ledger							 char(2);

	UPDATE 	work.sap

	/* ----------- ALTERAR ESSAS INFORMAÇÕES CONFORME O MES DE REFERENCIA PARA CALCULAR A RVR -------------*/ 
			
			/* ----------------- ALTERAR APENAS ESSES CAMPOS ----------------------*/ 

	SET		/*--> */ data_de_lancamento = &data.,  
			/*--> */ data_do_documento = &data.,   
			/*--> */ Referencia = &ref., 
			/*--> */ Texto_do_Item = "RVR DIRETO - &empresa. - &anomes.", 

			/* ----------------- CAMPO PADRÃO, NÃO DEVE SER ALTERADO --------*/ 
				tipo_de_conta = "S",  
				Ledger = "0L",
				Empresa = 20,  
				Periodo = &periodo.,
				Tipo_de_Documento = "LM", 
				Moeda ="BRL";
	QUIT; 

	/* CRIA O ARQUIVO FINAL QUE SERÁ IMPORTADO PARA O SAP								*/ 
	/* EXPORTAR O ARQUIVO E TRATAR AS COLUNAS 											*/

	PROC SQL; 

	CREATE TABLE SAP_FINAL AS 

	SELECT 			EMPRESA, 
					data_de_lancamento, 
					periodo, 
					data_do_documento,
					Tipo_de_Documento, 
					Fornecedor, 
					conta_contabil, 
					moeda, 
					filial, 
					centro_de_custo, 
					centro_de_lucro, 
					segmento, 
					tipo_de_movimento, 
					sociedade_parceira, 
					referencia, 
					texto_cabeçalho, 
					texto_do_item, 
					montante02 as montante, 
					chave_de_lancamento, 
					usuario, 
					tipo_de_conta, 
					ledger
	FROM 			WORK.SAP; 

	QUIT;


/*#################################################################################################################### */

/* EXPORT */

;
/* teste : Totais para comparar com exposição RVR original*/
;
/*
proc export 
	data=total_premiopendenteRVR_&anomes 
	outfile="&diretorio2.\total_base_premiopendenteRVR_DG_&anomes..csv"
	dbms=dlm;
	delimiter=";";
run;
*/
;
	PROC EXPORT

	DATA=SAP_FINAL
	outfile = "&diretorio2.\&anomes._mvida_direto_sap.xlsx" 
	DBMS=xlsx replace;
	sheet="SAP";

	RUN;  
;

/*

DATA AGING;
SET WORK.base_final;

DIAS_31 = (dt_vencimento - &mesref2);

	     	 IF     DIAS_31 >= 0   AND DIAS_31  <= 30 	THEN aging_dias_nome_31 = "av.000 a 030 dias";
	    ELSE IF     DIAS_31 > 30   AND DIAS_31  <= 60 	THEN aging_dias_nome_31 = "av.031 a 060 dias";
	    ELSE IF     DIAS_31 > 60   AND DIAS_31  <= 120 	THEN aging_dias_nome_31 = "av.061 a 120 dias";
	    ELSE IF     DIAS_31 > 120  AND DIAS_31  <= 180 	THEN aging_dias_nome_31 = "av.121 a 180 dias";
	    ELSE IF     DIAS_31 > 180  AND DIAS_31  <= 365	THEN aging_dias_nome_31 = "av.181 a 365 dias";
	    ELSE IF     DIAS_31 > 365 						THEN aging_dias_nome_31 = "av.acima de 365 dias"; 
	    ELSE IF     DIAS_31 < 0  	AND DIAS_31  >= -30	THEN aging_dias_nome_31 = "vc.001 a 030 dias venc"; 
	    ELSE IF     DIAS_31 < -30  AND DIAS_31  >= -60	THEN aging_dias_nome_31 = "vc.031 a 060 dias venc"; 
	    ELSE IF     DIAS_31 < -60  AND DIAS_31  >= -120	THEN aging_dias_nome_31 = "vc.061 a 120 dias venc";
	    ELSE IF     DIAS_31 < -120 AND DIAS_31  >= -180	THEN aging_dias_nome_31 = "vc.121 a 180 dias venc";
	    ELSE IF     DIAS_31 < -180 AND DIAS_31  >= -365	THEN aging_dias_nome_31 = "vc.181 a 365 dias venc";
	    ELSE IF     DIAS_31 < -365						THEN aging_dias_nome_31 = "vc.acima de 365 dias venc"; 
	    ELSE 		aging_dias_nome_31 ="Nenhuma das anteriores"; 

RUN;
*/

proc export 
	data=base_final
	outfile="&diretorio2.\&anomes._mvida_base_analitica_direto.csv"
	dbms=csv replace;
	delimiter=";";
run;
;
/*
;
libname RVR "&diretorio2.\";
data RVR.pendenteini_2; set pendenteini_2; run;
data RVR."&entcodigo._base_RVR_&anomes"n (compress=yes); set pendente_fnl; run;
;
*/



/*#################################################################################################################### */

/*ATÉ AQUI*/



/*#################################################################################################################### */

/*BASE CONCILIAÇÃO*/


DATA "&entcodigo._pend_analit_&anomes"n;
    LENGTH
        ano_mes            8
        cod_seguradora_susep   8
        cod_apolice      $ 30
        num_endosso      $ 14
        num_certificado  $ 16
        cod_produto        8
        cod_agencia        8
        cod_tp_emissao   $ 1
        cod_moeda          8
        dt_inicio_vigencia   8
        dt_fim_vigencia    8
        dt_emissao_doc     8
        qtde_parcelas      8
        dt_vencimento      8
        dt_inicio_cobertura_parcela   8
        dt_fim_cobertura_parcela   8
        num_parcela        8
        grupo_ramo_contabil   8
        cod_ramo_contabil   8
        cod_ramo_emitido   8
        val_cobranca       8
        val_iof            8
        val_custo_apolice   8
        val_desconto       8
        val_adic_fracionamento   8
        val_custo_inicial_contratacao   8
        val_comissao       8
        val_estipulante    8
        val_cobranca_cosseguro   8
        val_desconto_cosseguro   8
        val_adic_fracionamento_cosseguro   8
        val_comissao_cosseguro   8
        val_estipulante_cosseguro   8
        val_cobranca_resseguro   8
        val_comissao_resseguro   8
        val_direito_creditorio   8
        val_comissao_agenciamento   8
        val_remuneracao_representante   8
        dt_inicio_vigencia_ori   8
        dt_fim_vigencia_ori   8
        cod_sistema_origem $ 12
        cpf_cnpj_segurado   8
        AGENCIAMENTO_DEC   8
        aging_dias_nome  $ 25
        base_ppng          8
        CALC_DCD_DEC       8
        calc_ppng_dec      8
        CONDICOES        $ 10
        CONDICOES_V2     $ 10
        dcd_base           8
        DCD_RESSEGURO      8
        decorrer_e_decorrido $ 9
        DIAS               8
        DIAS_DECORRER      8
        DIAS_VIGENTE       8
        ESTIPULANTE_DEC    8
        ppng_resseguro     8
        premio_resseguro   8
        REMUNERACAO_DEC    8
        RVR                8
        venc_vinc        $ 8
        num_proposta     $ 17
        variacao_cambial   8
        AGING_DIAS_NOME $ 25
        RVR_ATUARIAL       8
        FATOR_ATUARIAL     8
        fator_atuarial_txt   8
        C1131900000        8
        C2125190000        8
        C2128900000        8
        C2122290000        8
        C2123190000        8
        C2123390000        8
        C2123590000        8
        C1132900002        8 ;
    FORMAT
        ano_mes          BEST6.
        cod_seguradora_susep BEST4.
        cod_apolice      $CHAR30.
        num_endosso      $CHAR14.
        num_certificado  $CHAR16.
        cod_produto      BEST4.
        cod_agencia      BEST4.
        cod_tp_emissao   $CHAR1.
        cod_moeda        BEST3.
        dt_inicio_vigencia DDMMYY10.
        dt_fim_vigencia  DDMMYY10.
        dt_emissao_doc   DDMMYY10.
        qtde_parcelas    BEST3.
        dt_vencimento    DDMMYY10.
        dt_inicio_cobertura_parcela MMDDYY10.
        dt_fim_cobertura_parcela MMDDYY10.
        num_parcela      BEST3.
        grupo_ramo_contabil BEST2.
        cod_ramo_contabil BEST2.
        cod_ramo_emitido BEST2.
        val_cobranca     BEST13.
        val_iof          BEST12.
        val_custo_apolice BEST9.
        val_desconto     BEST9.
        val_adic_fracionamento BEST8.
        val_custo_inicial_contratacao BEST4.
        val_comissao     BEST10.
        val_estipulante  BEST10.
        val_cobranca_cosseguro BEST12.
        val_desconto_cosseguro BEST4.
        val_adic_fracionamento_cosseguro BEST4.
        val_comissao_cosseguro BEST10.
        val_estipulante_cosseguro BEST4.
        val_cobranca_resseguro BEST10.
        val_comissao_resseguro BEST9.
        val_direito_creditorio BEST13.
        val_comissao_agenciamento BEST8.
        val_remuneracao_representante BEST8.
        dt_inicio_vigencia_ori DDMMYY10.
        dt_fim_vigencia_ori DDMMYY10.
        cod_sistema_origem $CHAR12.
        cpf_cnpj_segurado BEST14.
        AGENCIAMENTO_DEC BEST6.
        aging_dias_nome  $CHAR25.
        base_ppng        BEST13.
        CALC_DCD_DEC     BEST11.
        calc_ppng_dec    BEST13.
        CONDICOES        $CHAR10.
        CONDICOES_V2     $CHAR10.
        dcd_base         BEST11.
        DCD_RESSEGURO    BEST6.
        decorrer_e_decorrido $CHAR9.
        DIAS             BEST5.
        DIAS_DECORRER    BEST9.
        DIAS_VIGENTE     BEST9.
        ESTIPULANTE_DEC  BEST9.
        ppng_resseguro   BEST13.
        premio_resseguro BEST15.
        REMUNERACAO_DEC  BEST6.
        RVR              BEST12.
        venc_vinc        $CHAR8.
        num_proposta     $CHAR17.
        variacao_cambial BEST10.
        AGING_DIAS_NOME $CHAR25.
        RVR_ATUARIAL     BEST12.
        FATOR_ATUARIAL   BEST6.
        fator_atuarial_txt BEST6.
        C1131900000      BEST14.
        C2125190000      BEST12.
        C2128900000      BEST11.
        C2122290000      BEST12.
        C2123190000      BEST14.
        C2123390000      BEST12.
        C2123590000      BEST11.
        C1132900002      BEST11. ;
    INFORMAT
        ano_mes          BEST6.
        cod_seguradora_susep BEST4.
        cod_apolice      $CHAR30.
        num_endosso      $CHAR14.
        num_certificado  $CHAR16.
        cod_produto      BEST4.
        cod_agencia      BEST4.
        cod_tp_emissao   $CHAR1.
        cod_moeda        BEST3.
        dt_inicio_vigencia DDMMYY10.
        dt_fim_vigencia  DDMMYY10.
        dt_emissao_doc   DDMMYY10.
        qtde_parcelas    BEST3.
        dt_vencimento    DDMMYY10.
        dt_inicio_cobertura_parcela MMDDYY10.
        dt_fim_cobertura_parcela MMDDYY10.
        num_parcela      BEST3.
        grupo_ramo_contabil BEST2.
        cod_ramo_contabil BEST2.
        cod_ramo_emitido BEST2.
        val_cobranca     BEST13.
        val_iof          BEST12.
        val_custo_apolice BEST9.
        val_desconto     BEST9.
        val_adic_fracionamento BEST8.
        val_custo_inicial_contratacao BEST4.
        val_comissao     BEST10.
        val_estipulante  BEST10.
        val_cobranca_cosseguro BEST12.
        val_desconto_cosseguro BEST4.
        val_adic_fracionamento_cosseguro BEST4.
        val_comissao_cosseguro BEST10.
        val_estipulante_cosseguro BEST4.
        val_cobranca_resseguro BEST10.
        val_comissao_resseguro BEST9.
        val_direito_creditorio BEST13.
        val_comissao_agenciamento BEST8.
        val_remuneracao_representante BEST8.
        dt_inicio_vigencia_ori DDMMYY10.
        dt_fim_vigencia_ori DDMMYY10.
        cod_sistema_origem $CHAR12.
        cpf_cnpj_segurado BEST14.
        AGENCIAMENTO_DEC BEST6.
        aging_dias_nome  $CHAR25.
        base_ppng        BEST13.
        CALC_DCD_DEC     BEST11.
        calc_ppng_dec    BEST13.
        CONDICOES        $CHAR10.
        CONDICOES_V2     $CHAR10.
        dcd_base         BEST11.
        DCD_RESSEGURO    BEST6.
        decorrer_e_decorrido $CHAR9.
        DIAS             BEST5.
        DIAS_DECORRER    BEST9.
        DIAS_VIGENTE     BEST9.
        ESTIPULANTE_DEC  BEST9.
        ppng_resseguro   BEST13.
        premio_resseguro BEST15.
        REMUNERACAO_DEC  BEST6.
        RVR              BEST12.
        venc_vinc        $CHAR8.
        num_proposta     $CHAR17.
        variacao_cambial BEST10.
        AGING_DIAS_NOME $CHAR25.
        RVR_ATUARIAL     BEST12.
        FATOR_ATUARIAL   BEST6.
        fator_atuarial_txt BEST6.
        C1131900000      BEST14.
        C2125190000      BEST12.
        C2128900000      BEST11.
        C2122290000      BEST12.
        C2123190000      BEST14.
        C2123390000      BEST12.
        C2123590000      BEST11.
        C1132900002      BEST11. ;
    INFILE '\\vbr001001-572\dados_contabil\00 BASE\02.MVIDA\00 Arquivos_sas\&anomes._mvida_base_analitica_direto.csv'
		FIRSTOBS=2
        LRECL=634
        ENCODING="WLATIN1"
        TERMSTR=CRLF
        DLM=';'
        MISSOVER
        DSD ;
    INPUT
        ano_mes          : ?? BEST6.
        cod_seguradora_susep : ?? BEST4.
        cod_apolice      : $CHAR30.
        num_endosso      : $CHAR14.
        num_certificado  : $CHAR16.
        cod_produto      : ?? BEST4.
        cod_agencia      : ?? BEST4.
        cod_tp_emissao   : $CHAR1.
        cod_moeda        : ?? BEST3.
        dt_inicio_vigencia : ?? DDMMYY10.
        dt_fim_vigencia  : ?? DDMMYY10.
        dt_emissao_doc   : ?? DDMMYY10.
        qtde_parcelas    : ?? BEST3.
        dt_vencimento    : ?? DDMMYY10.
        dt_inicio_cobertura_parcela : ?? MMDDYY10.
        dt_fim_cobertura_parcela : ?? MMDDYY10.
        num_parcela      : ?? BEST3.
        grupo_ramo_contabil : ?? BEST2.
        cod_ramo_contabil : ?? BEST2.
        cod_ramo_emitido : ?? BEST2.
        val_cobranca     : ?? COMMAX13.
        val_iof          : ?? COMMAX12.
        val_custo_apolice : ?? COMMAX9.
        val_desconto     : ?? COMMAX9.
        val_adic_fracionamento : ?? COMMAX8.
        val_custo_inicial_contratacao : ?? COMMAX4.
        val_comissao     : ?? COMMAX10.
        val_estipulante  : ?? COMMAX10.
        val_cobranca_cosseguro : ?? COMMAX12.
        val_desconto_cosseguro : ?? COMMAX4.
        val_adic_fracionamento_cosseguro : ?? COMMAX4.
        val_comissao_cosseguro : ?? COMMAX10.
        val_estipulante_cosseguro : ?? COMMAX4.
        val_cobranca_resseguro : ?? COMMAX10.
        val_comissao_resseguro : ?? COMMAX9.
        val_direito_creditorio : ?? COMMAX13.
        val_comissao_agenciamento : ?? COMMAX8.
        val_remuneracao_representante : ?? COMMAX8.
        dt_inicio_vigencia_ori : ?? DDMMYY10.
        dt_fim_vigencia_ori : ?? DDMMYY10.
        cod_sistema_origem : $CHAR12.
        cpf_cnpj_segurado : ?? BEST14.
        AGENCIAMENTO_DEC : ?? COMMAX6.
        aging_dias_nome  : $CHAR25.
        base_ppng        : ?? COMMAX13.
        CALC_DCD_DEC     : ?? COMMAX11.
        calc_ppng_dec    : ?? COMMAX13.
        CONDICOES        : $CHAR10.
        CONDICOES_V2     : $CHAR10.
        dcd_base         : ?? COMMAX11.
        DCD_RESSEGURO    : ?? COMMAX6.
        decorrer_e_decorrido : $CHAR9.
        DIAS             : ?? BEST5.
        DIAS_DECORRER    : ?? COMMAX9.
        DIAS_VIGENTE     : ?? COMMAX9.
        ESTIPULANTE_DEC  : ?? COMMAX9.
        ppng_resseguro   : ?? COMMAX13.
        premio_resseguro : ?? COMMAX15.
        REMUNERACAO_DEC  : ?? COMMAX6.
        RVR              : ?? COMMAX12.
        venc_vinc        : $CHAR8.
        num_proposta     : $CHAR17.
        variacao_cambial : ?? COMMA10.
        AGING_DIAS_NOME : $CHAR25.
        RVR_ATUARIAL     : ?? COMMAX12.
        FATOR_ATUARIAL   : ?? COMMAX6.
        fator_atuarial_txt : ?? COMMA6.
        C1131900000      : ?? COMMAX14.
        C2125190000      : ?? COMMAX12.
        C2128900000      : ?? COMMAX11.
        C2122290000      : ?? COMMAX12.
        C2123190000      : ?? COMMAX14.
        C2123390000      : ?? COMMAX12.
        C2123590000      : ?? COMMAX11.
        C1132900002      : ?? COMMAX11. ;
RUN;


PROC SQL;
   CREATE TABLE WORK.BASE_CONCILIACAO AS 
   SELECT t1.CONDICOES_V2, 
          t1.grupo_ramo_contabil, 
          t1.cod_ramo_contabil, 
          t1.cod_produto, 
          t1.cod_sistema_origem, 
          t1.AGING_DIAS_NOME, 
          t1.aging_dias_nome, 
          t1.FATOR_ATUARIAL, 
            (SUM(t1.val_cobranca)) FORMAT=BEST13. AS SUM_of_val_cobranca, 
            (SUM(t1.val_iof)) FORMAT=BEST12. AS SUM_of_val_iof, 
            (SUM(t1.val_custo_apolice)) FORMAT=BEST9. AS SUM_of_val_custo_apolice, 
            (SUM(t1.val_desconto)) FORMAT=BEST9. AS SUM_of_val_desconto, 
            (SUM(t1.val_adic_fracionamento)) FORMAT=BEST8. AS SUM_of_val_adic_fracionamento, 
            (SUM(t1.val_custo_inicial_contratacao)) FORMAT=BEST4. AS SUM_of_val_custo_inicial_contrat, 
            (SUM(t1.val_comissao)) FORMAT=BEST10. AS SUM_of_val_comissao, 
            (SUM(t1.val_estipulante)) FORMAT=BEST10. AS SUM_of_val_estipulante, 
            (SUM(t1.val_cobranca_cosseguro)) FORMAT=BEST12. AS SUM_of_val_cobranca_cosseguro, 
            (SUM(t1.val_desconto_cosseguro)) FORMAT=BEST4. AS SUM_of_val_desconto_cosseguro, 
            (SUM(t1.val_adic_fracionamento_cosseguro)) FORMAT=BEST4. AS SUM_of_val_adic_fracionamento_co, 
            (SUM(t1.val_comissao_cosseguro)) FORMAT=BEST10. AS SUM_of_val_comissao_cosseguro, 
            (SUM(t1.val_estipulante_cosseguro)) FORMAT=BEST4. AS SUM_of_val_estipulante_cosseguro, 
            (SUM(t1.val_comissao_agenciamento)) FORMAT=BEST8. AS SUM_of_val_comissao_agenciamento, 
            (SUM(t1.val_remuneracao_representante)) FORMAT=BEST8. AS SUM_of_val_remuneracao_represent, 
            (SUM(t1.AGENCIAMENTO_DEC)) FORMAT=BEST6. AS SUM_of_AGENCIAMENTO_DEC, 
            (SUM(t1.CALC_DCD_DEC)) FORMAT=BEST11. AS SUM_of_CALC_DCD_DEC, 
            (SUM(t1.calc_ppng_dec)) FORMAT=BEST13. AS SUM_of_calc_ppng_dec, 
            (SUM(t1.DCD_RESSEGURO)) FORMAT=BEST6. AS SUM_of_DCD_RESSEGURO, 
            (SUM(t1.ESTIPULANTE_DEC)) FORMAT=BEST9. AS SUM_of_ESTIPULANTE_DEC, 
            (SUM(t1.ppng_resseguro)) FORMAT=BEST13. AS SUM_of_ppng_resseguro, 
            (SUM(t1.premio_resseguro)) FORMAT=BEST15. AS SUM_of_premio_resseguro, 
            (SUM(t1.REMUNERACAO_DEC)) FORMAT=BEST6. AS SUM_of_REMUNERACAO_DEC, 
            (SUM(t1.RVR)) FORMAT=BEST12. AS SUM_of_RVR, 
            (SUM(t1.RVR_ATUARIAL)) FORMAT=BEST12. AS SUM_of_RVR_ATUARIAL, 
            (SUM(t1.C1131900000)) FORMAT=BEST14. AS SUM_of_C1131900000, 
            (SUM(t1.C2125190000)) FORMAT=BEST12. AS SUM_of_C2125190000, 
            (SUM(t1.C2128900000)) FORMAT=BEST11. AS SUM_of_C2128900000, 
            (SUM(t1.C2122290000)) FORMAT=BEST12. AS SUM_of_C2122290000, 
            (SUM(t1.C2123190000)) FORMAT=BEST14. AS SUM_of_C2123190000, 
            (SUM(t1.C2123390000)) FORMAT=BEST12. AS SUM_of_C2123390000, 
            (SUM(t1.C2123590000)) FORMAT=BEST11. AS SUM_of_C2123590000, 
            (SUM(t1.C1132900002)) FORMAT=BEST11. AS SUM_of_C1132900002
      FROM "&entcodigo._pend_analit_&anomes"n t1
      GROUP BY t1.CONDICOES_V2,
               t1.grupo_ramo_contabil,
               t1.cod_ramo_contabil,
               t1.cod_produto,
               t1.cod_sistema_origem,
               t1.AGING_DIAS_NOME,
               t1.aging_dias_nome,
               t1.FATOR_ATUARIAL;
QUIT;


PROC EXPORT

DATA=WORK.BASE_CONCILIACAO
OUTFILE = "\\vbr001001-572\dados_contabil\00 BASE\02.MVIDA\00 Arquivos_sas\mvida_Base_RVR_Conciliação_&anomes..xlsx"
DBMS=xlsx replace;

RUN; 
