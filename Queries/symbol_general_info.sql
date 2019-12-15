

create view symbol_general_info as
select a.*, c.countryba, c.sic, d.office, d.industry
	from (select distinct * from symbols) a
	left join sec_cik_symbol b
	on a.symbol = b.symbol
	left join (select distinct cik, sic, countryba from sec_sub) c
	on b.cik = c.cik
	left join sec_sic_industry d
	on d.sic = c.sic
order by a.symbol
