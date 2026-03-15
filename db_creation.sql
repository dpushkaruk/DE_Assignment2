insert into employees (employee_id, full_name, team, role, hire_date)
with recursive seq as (
    select 1 as n union all select n + 1 from seq where n < 50
)
select
    n,
    concat(
            elt(floor(1 + (rand() * 10)), 'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'David', 'Elizabeth'),
            ' ',
            elt(floor(1 + (rand() * 10)), 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez')
    ) as full_name,
    elt(floor(1 + (rand() * 4)), 'Billing', 'Tech_Support', 'Retention', 'General_Support') as team,
    elt(floor(1 + (rand() * 3)), 'Junior_Agent', 'Senior_Agent', 'Team_Lead') as role,
    date_sub(curdate(), interval floor(rand() * 1000) day) as hire_date
from seq;

insert into calls (call_id, employee_id, call_time, phone, direction, status)
with recursive seq as (
    select 1 as n
    union all
    select n + 1
    from seq
    where n < 50
)
select
    n as call_id,
    floor(1 + (rand() * 50)) as employee_id,
    timestampadd(minute, -floor(rand() * 24 * 60 * 4), now()) as call_time,
    concat('+380-67-', lpad(floor(rand() * 1000), 8, '0')) as phone,
    if(rand() < 0.65, 'inbound', 'outbound') as direction,
    case
        when rand() < 0.65 then 'completed'
        when rand() < 0.70 then 'dropped'
        when rand() < 0.85 then 'voicemail'
        else 'missed'
        end as status
from seq;
