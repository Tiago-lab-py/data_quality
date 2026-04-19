SELECT
  TO_CHAR(HCAI.num_uc_hcai) AS NUM_UC_UCI,
  TO_CHAR(HCAI.indic_reg_orig_intrp_hcai) AS SIGLA_REGIONAL,
  TO_CHAR(HCAI.num_intrp_hcai) AS NUM_INTRP_UCI,
  TO_CHAR(intrp_ini.TIPO_PROTOC_JUSTIF_INTRP) AS TIPO_PROTOC_JUSTIF_UCI,
  TO_CHAR(intrp_ini.data_hora_inic_intrp, 'YYYY-MM-DD HH24:MI:SS') AS DTHR_INICIO_INTRP_UC,
  TO_CHAR(intrp_fim.data_hora_fim_intrp, 'YYYY-MM-DD HH24:MI:SS') AS DATA_HORA_FIM_INTRP,
  TO_CHAR(NVL(hcai.num_intrp_inic_manobra_hcai, HCAI.num_intrp_hcai)) AS NUM_INTRP_INIC_MANOBRA_UCI,
  TO_CHAR((intrp_fim.data_hora_fim_intrp - intrp_ini.data_hora_inic_intrp) * 24 * 60, 'FM9999999999999990.000') AS DURACAO_PERCEBIDA_MINUTOS
FROM
  iqs.hist_cons_afetado_interrupcao HCAI
  LEFT JOIN sod.interrupcao intrp_ini
    ON HCAI.indic_reg_orig_intrp_hcai = intrp_ini.indic_reg_orig_intrp
   AND NVL(hcai.num_intrp_inic_manobra_hcai, HCAI.num_intrp_hcai) = intrp_ini.NUM_SEQ_INTRP
  LEFT JOIN sod.interrupcao intrp_fim
    ON HCAI.indic_reg_orig_intrp_hcai = intrp_fim.indic_reg_orig_intrp
   AND HCAI.num_intrp_hcai = intrp_fim.NUM_SEQ_INTRP
WHERE
  HCAI.indic_fat_hcai = 'S'
 AND to_char(intrp_ini.DATA_HORA_INIC_INTRP, 'YYYYMM') = :data_mes
