/* Risk Model Scoring across segments */
LIBNAME dw '/sas/data/dw';
LIBNAME output '/sas/data/output';

%MACRO score_segment(segment);
  /* Filter segment */
  DATA work.seg_&segment;
    SET dw.dim_customer;
    WHERE segment = "&segment";
  RUN;

  /* Univariate analysis */
  PROC UNIVARIATE DATA=work.seg_&segment;
    VAR income risk_score;
  RUN;

  /* Logistic regression */
  PROC LOGISTIC DATA=work.seg_&segment;
    MODEL status(EVENT='ACTIVE') = income risk_score / SELECTION=STEPWISE;
    OUTPUT OUT=work.scored_&segment PREDICTED=pred_prob;
  RUN;

  /* Frequency analysis */
  PROC FREQ DATA=work.scored_&segment;
    TABLES status / NOCUM;
  RUN;
%MEND score_segment;

%score_segment(RETAIL);
%score_segment(CORPORATE);
%score_segment(SME);

/* Combine all scored segments */
DATA output.risk_scores;
  SET work.scored_RETAIL
      work.scored_CORPORATE
      work.scored_SME;
RUN;

PROC MEANS DATA=output.risk_scores N MEAN STD;
  VAR pred_prob;
  CLASS segment;
RUN;
