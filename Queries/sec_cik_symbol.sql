
CREATE TABLE sec_cik_symbol
(
	cik int NOT NULL,
	symbol character varying(10) NOT NULL,
	name text NULL,
	CONSTRAINT cik PRIMARY KEY (cik)
)

--COPY sec_cik_symbol(cik, symbol, name)
--FROM '\sec_cik_symbol.csv' DELIMITER ',' CSV HEADER;
