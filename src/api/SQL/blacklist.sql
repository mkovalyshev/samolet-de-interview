select count(*) as blacklist
from public.blacklist
where True
    and phone = '{}'
;