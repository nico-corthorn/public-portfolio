
--drop table reg_factors

CREATE TABLE public.reg_factors
(
    symbol character varying(10) COLLATE pg_catalog."default" NOT NULL,
    date date NOT NULL,
	ret real,
	equity bigint,
    mcap bigint,
    pb real,
    mom real,
	PRIMARY KEY (symbol, date),
    FOREIGN KEY (symbol)
        REFERENCES public.symbols (symbol) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
)