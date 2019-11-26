-- Table: public.prices

-- DROP TABLE public.prices;

CREATE TABLE public.prices_complete
(
	symbol character varying(10) NOT NULL,
    date date NOT NULL,
    open numeric(14,2) NOT NULL,
    close numeric(14,2) NOT NULL,
	volume integer NOT NULL,
    adjClose numeric(14,2) NULL,
    divCash numeric(14,1) NULL,
    split numeric(14,1) NULL,
	PRIMARY KEY (symbol, date),
    FOREIGN KEY (symbol) REFERENCES symbols (symbol) ON DELETE CASCADE
)