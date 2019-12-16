
--drop table reg_factors_scaled

CREATE TABLE public.reg_factors_scaled
(
    symbol character varying(10) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
	ret real,
    mcap real,
    pb real,
    mom real,
	weight real,
	PRIMARY KEY (symbol, date),
    FOREIGN KEY (symbol)
        REFERENCES public.symbols (symbol) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)
