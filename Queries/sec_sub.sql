
CREATE TABLE public.sec_sub
(
	adsh character varying(20) NOT NULL,
	cik int NOT NULL,
	name character varying(150) NULL,
 	sic int NULL,
	countryba character varying(2) NULL,
	stprba character varying(2) NULL,
	fye character varying(4) NOT NULL,
	form character varying(10) NOT NULL,
	period character varying(8) NULL,
	fy int NULL,
	fp character varying(2) NULL,
	filed character varying(8) NULL,
	query character varying(6) NOT NULL,
    CONSTRAINT sec_sub_pkey PRIMARY KEY (adsh)
)