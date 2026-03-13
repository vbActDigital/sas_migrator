
DATA BASE_RVR;
	set pendente_fnl (keep=
        ano_mes            
        cod_seguradora_susep   
        cod_apolice      
        num_endosso      
        num_certificado  
        cod_produto        
        cod_agencia        
        cod_tp_emissao   
        cod_moeda          
        dt_inicio_vigencia   
        dt_fim_vigencia    
        dt_emissao_doc     
        qtde_parcelas     
        dt_vencimento      
        dt_inicio_cobertura_parcela   
        dt_fim_cobertura_parcela   
        num_parcela        
        grupo_ramo_contabil   
        cod_ramo_contabil   
        cod_ramo_emitido   
        val_cobranca       
        val_iof            
        val_custo_apolice   
        val_desconto       
        val_adic_fracionamento   
        val_custo_inicial_contratacao   
        val_comissao       
        val_estipulante    
        val_cobranca_cosseguro   
        val_desconto_cosseguro   
        val_adic_fracionamento_cosseguro   
        val_comissao_cosseguro   
        val_estipulante_cosseguro   
        val_cobranca_resseguro   
        val_comissao_resseguro   
        val_direito_creditorio   
        val_comissao_agenciamento   
        val_remuneracao_representante   
        dt_inicio_vigencia_ori   
        dt_fim_vigencia_ori   
        cod_sistema_origem
        cpf_cnpj_segurado   
        AGENCIAMENTO_DEC   
        aging_dias_nome  
        base_ppng          
        CALC_DCD_DEC       
        calc_ppng_dec      
        CONDICOES        
        CONDICOES_V2     
        dcd_base           
        DCD_RESSEGURO      
        decorrer_e_decorrido 
        DIAS               
        DIAS_DECORRER      
        DIAS_VIGENTE       
        ESTIPULANTE_DEC    
        ppng_resseguro     
        premio_resseguro   
        REMUNERACAO_DEC    
        RVR                
        venc_vinc        
        num_proposta     
        AGING_DIAS_NOME_V2
        RVR_ATUARIAL       
        FATOR_ATUARIAL
		IDLG);
        fator_atuarial_txt="";
        C1131900000="";
        C2125190000="";
        C2128900000="";
        C2122290000="";
        C2123190000="";
        C2123390000="";
        C2123590000="";
        C1132900002="";

		RUN;



	PROC EXPORT

	DATA=BASE_RVR
	OUTFILE = "\\vbr001001-572\dados_contabil\00 Base\01.MSG\00 Arquivos_sas\202602_msg_direto_base_analitica_fin.csv"
	DBMS=CSV replace;
	DELIMITER=';'; 

	RUN; 


	PROC EXPORT

	DATA=BASE_RVR
	OUTFILE = "\\vbr001001-572\dados_contabil\00 BASE\02.MVIDA\00 Arquivos_sas\202602_mvida_direto_base_analitica_fin.csv"
	DBMS=CSV replace;
	DELIMITER=';'; 

	RUN; 


	
PROC SQL;
CREATE TABLE WORK.BASE_PESQUISA AS 
SELECT * 
FROM WORK.BASE_RVR
WHERE 	
venc_vinc = "Vencido";

QUIT; 


PROC EXPORT

DATA=BASE_PESQUISA
OUTFILE = "\\vbr001001-572\dados_contabil\00 BASE\01.MSG\00 Arquivos_sas\202602_msg_direto_base_analitica_vencido.csv"
DBMS=CSV replace;
DELIMITER=';'; 

RUN; 