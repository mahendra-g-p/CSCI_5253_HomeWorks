-- How many animals of each type have outcomes?
-- I.e. how many cats, dogs, birds etc. 
-- Note that this question is asking about number of animals, not number of outcomes, so animals with multiple outcomes should be counted only once.


select
	animal_type,
	COUNT(distinct animal_id) as unique_animal_ids_with_outcomes
from
	public.animal_dimension ad,
	public.outcomes_fact of2 ,
	public.outcome_type_dimension otd
where
	ad.animal_key = of2.animal_key
	and of2.outcome_type_key = otd.outcome_type_key
	and otd.outcome_type not in ('Not_Available')
group by
	ad.animal_type;


-- How many animals are there with more than 1 outcome?

select
	COUNT(animal_id) as count_of_animals_with_multiple_outcomes
from
	(
	select
		ad.animal_id
	from
		public.animal_dimension ad,
		public.outcomes_fact of2 ,
		public.outcome_type_dimension otd
	where
		ad.animal_key = of2.animal_key
		and of2.outcome_type_key = otd.outcome_type_key
		and otd.outcome_type not in ('Not_Available')
	group by
		animal_id
	having
		COUNT(*) > 1
) as query;

-- What are the top 5 months for outcomes? 
-- Calendar months in general, not months of a particular year. 
-- This means answer will be like April, October, etc rather than April 2013, October 2018, 


select
	dd.month_recorded,
	COUNT(dd.month_recorded) as outcome_count
from
	public.date_dimension dd,
	public.outcomes_fact of2,
	public.outcome_type_dimension otd
where
	dd.date_key = of2.date_key
	and of2.outcome_type_key = otd.outcome_type_key
	and otd.outcome_type not in ('Not_Available')
group by
	dd.month_recorded
order by
	outcome_count desc
limit 5;

-- A "Kitten" is a "Cat" who is less than 1 year old. 
-- A "Senior cat" is a "Cat" who is over 10 years old.
-- An "Adult" is a cat who is between 1 and 10 years old.
-- What is the total number of kittens, adults, and seniors, whose outcome is "Adopted"?


select
	sum(count) as totalcatsadopted
from
	(
	select
		age_group,
		COUNT(*) as count
	from
		(
		select
			animal_key,
			case
				when AGE(dob) < interval '1 year' then 'Kitten'
				when AGE(dob) >= interval '1 year'
				and AGE(dob) <= interval '10 years' then 'Adult'
				when AGE(dob) > interval '10 years' then 'Senior'
				else 'Unknown'
			end as age_group
		from
			public.animal_dimension ad
		where
			ad.animal_type = 'Cat'
) as cat_age_groups
	join public.outcomes_fact of2 on
		cat_age_groups.animal_key = of2.animal_key
	join outcome_type_dimension otd on
		of2.outcome_type_key = otd.outcome_type_key
	where
		otd.outcome_type = 'Adoption'
	group by
		age_group
);



-- Conversely, among all the cats who were "Adopted", 
-- what is the total number of kittens, adults, and seniors?
with Cat_Age_Groups as (
select
	animal_key,
	case
		when AGE(dob) < interval '1 year' then 'Kitten'
		when AGE(dob) >= interval '1 year'
		and AGE(dob) <= interval '10 years' then 'Adult'
		when AGE(dob) > interval '10 years' then 'Senior'
		else 'Unknown'
	end as age_group
from
	public.animal_dimension ad
where
	ad.animal_type = 'Cat'
)

select
	age_group,
	COUNT(*) as count
from
	public.outcomes_fact of2
join Cat_Age_Groups cats on
	of2.animal_key = cats.animal_key
join outcome_type_dimension otd on
	of2.outcome_type_key = otd.outcome_type_key
where
	otd.outcome_type = 'Adoption'
group by
	age_group;




-- For each date, what is the cumulative total of outcomes up to and including this date?

with DailyOutcomes as (
select 
	dd.date_recorded,
	COUNT(*) as daily_count
from
	public.date_dimension dd
left join public.outcomes_fact ofact on
	dd.date_key = ofact.date_key
left join public.outcome_type_dimension otd on
	ofact.outcome_type_key = otd.outcome_type_key
where
	otd.outcome_type not in ('Not_Available')
group by
	dd.date_recorded
order by
	dd.date_recorded asc

)
select
	doc.date_recorded,
	SUM(doc.daily_count) over (
order by
	doc.date_recorded) as cumulative_total
from
	DailyOutcomes doc;