
CREATE TABLE sec_sic_industry
(
	sic int NOT NULL,
	office character varying(100) NOT NULL,
	industry text NOT NULL,
	CONSTRAINT sik PRIMARY KEY (sic)
)

--COPY sec_sic_industry(sic, office, industry)
--FROM 'D:\Google Drive\Miscelaneo\Proyectos\Acciones\public-portfolio\Data\sec_sic_industry.csv' DELIMITER ',' CSV HEADER;
