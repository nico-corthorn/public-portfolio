
create view symbols_last_date as
select a.symbol, b.last_trade_date
from symbols a
left join
(
	select symbol, max(trade_date) last_trade_date from prices group by symbol
) b
on b.symbol = a.symbol
order by symbol
;