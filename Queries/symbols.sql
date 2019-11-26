
CREATE TABLE symbols
(
	symbol character varying(10) NOT NULL,
	company_name text,
	CONSTRAINT symbol PRIMARY KEY (symbol)
)

--COPY symbols(symbol, company_name)
--FROM '\data_symbols.csv' DELIMITER ',' CSV HEADER;
