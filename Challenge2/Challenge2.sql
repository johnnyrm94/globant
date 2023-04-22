-- Challenge 2
-- Query 1

select d.department, j.job, 
       count(case when DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 1 then he.id end) as Q1,
       count(case when DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2 then he.id end) as Q2,
       count(case when DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 3 then he.id end) as Q3,
       count(case when DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 4 then he.id end) as Q4
from hired_employees he 
inner join jobs j on  he.job_id = j.id
inner join departments d on he.department_id = d.id 
where EXTRACT(YEAR FROM TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2021
group by d.department, j.job
order by d.department,j.job asc ;

-- Tableau Graph for Query 1: https://us-west-2b.online.tableau.com/t/johnnyrm94/views/globant/Query1

--Query 2

with amount_hired as (
	select count(id) amount, department_id
	from hired_employees
	where EXTRACT(YEAR FROM TO_TIMESTAMP(datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2021
	group by department_id
), 

avg_hired as (
	select AVG(amount) average
	from amount_hired
)

select am.department_id id, d.department, am.amount hired
from amount_hired am 
inner join avg_hired av on am.amount > av.average
inner join departments d  on am.department_id = d.id
order by am.amount desc;

-- Tableau Graph for Query 2: https://us-west-2b.online.tableau.com/t/johnnyrm94/views/globant/Query2