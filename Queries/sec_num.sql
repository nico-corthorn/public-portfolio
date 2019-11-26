
CREATE TABLE public.sec_num
(
	adsh character varying(20) NOT NULL,
	tag character varying(256) NOT NULL,
	version character varying(20) NOT NULL,
	ddate character varying(8) NOT NULL,
	qtrs int NOT NULL,
	uom character varying(20) NOT NULL,
	coreg character varying(256) NULL,
	value NUMERIC(28,4) NOT NULL,
    CONSTRAINT sec_num_pkey PRIMARY KEY (adsh, tag, version, ddate, qtrs, uom, coreg)
)