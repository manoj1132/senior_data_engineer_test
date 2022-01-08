-- total messages sent everyday

select created_at, count(1)
from messages
group by 1;

--users who did not receive any messages
select * from users
where id not in (select distinct receiver_id from messages);

--active subscriptions
select count(distinct id) 
from subscriptions
where status = 'Active';

-- users sending messages withour active subscriptions
select * 
from users
where id in (select distinct sender_id from messages)
and id not in (select distinct id from subscriptions where status = 'Active';

--inaccurate/noisy records
-- I obsereved one users which does not have any active subscription but sending/receiving
-- query given below

select * from users
where (id in (select distinct sender_id from messages)
or id in (select distinct receiver_id from messages))
and id in (select distinct id from subscription where status in ['Inactive', 'Rejected'));
