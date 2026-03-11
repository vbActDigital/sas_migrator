/* ETL: Load and transform customer data */
LIBNAME rawdata '/sas/data/raw';
LIBNAME dw '/sas/data/dw';

%MACRO log_step(step_name);
  %PUT NOTE: Step &step_name started at %SYSFUNC(datetime(), datetime20.);
%MEND log_step;

%log_step(load_customers);

/* Dedup customers */
PROC SORT DATA=rawdata.customers_raw OUT=work.customers_sorted NODUPKEY;
  BY customer_id;
RUN;

/* Remove duplicates using FIRST. */
DATA work.customers_dedup;
  SET work.customers_sorted;
  BY customer_id;
  IF FIRST.customer_id;
RUN;

/* Merge with addresses */
DATA work.customers_with_addr;
  MERGE work.customers_dedup (IN=a)
        rawdata.addresses (IN=b);
  BY customer_id;
  IF a;
RUN;

/* Create dim_customer */
PROC SQL;
  CREATE TABLE dw.dim_customer AS
  SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.cpf,
    c.phone,
    c.birth_date,
    c.gender,
    c.income,
    c.segment,
    c.risk_score,
    a.city,
    a.state,
    a.zip_code,
    c.registration_date,
    c.status
  FROM work.customers_dedup c
  LEFT JOIN rawdata.addresses a ON c.customer_id = a.customer_id;
QUIT;

PROC FREQ DATA=dw.dim_customer;
  TABLES segment * status / NOCUM;
RUN;

PROC MEANS DATA=dw.dim_customer N MEAN STD MIN MAX;
  VAR income risk_score;
RUN;

%log_step(load_customers_done);
