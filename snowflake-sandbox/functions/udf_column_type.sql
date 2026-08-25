	select
	(case 
        when col = 'TEXT' and p < 16777216 then 'VARCHAR(' || p || ')'
        when col = 'NUMBER' and p <> 38 and s <> 0 
            then 'NUMBER(' || p ||', ' || s || ')'
        when col = 'TIME' and p < 9 then 'TIME(' || p || ')'
        when col = 'TIMESTAMP_NTZ' and p = 9 then 'TIMESTAMP'
        when col = 'TIMESTAMP_NTZ' and p < 9 then 'TIMESTAMP(' || p || ')'
        when col in ('TIMESTAMP_LTZ', 'TIMESTAMP_TZ') and p < 9 then col || '(' || p || ')'
        when col = 'USER_DEFINED_TYPE' then 'UNKNOWN'
        else col
        end)