-- Distinct year and how many disaster happened in each year
select year,count(year) as Number_of_disasters_per_year from natural_disasters
group by(year)
order by year desc;

-- Distinct disaster subgroups and how many disasters are in each subgroup
select Disaster_Subgroup, count(Disaster_Subgroup) as Count_for_Disster_Subgroup from natural_disasters
group by Disaster_Subgroup
order by 2 desc;

-- Counting the number of times a disaster subgroup occurs in each year 
select distinct(year), Disaster_Subgroup,
count(*) over(partition by Disaster_Subgroup,year) as Count_for_Disaster_Subgroup_per_year
from natural_disasters
order by 1;

-- Ranking the number of times a disaster subgroup occurs within each year using ctes 
with distinct_subgroup as(
select distinct(year), Disaster_Subgroup,
count(*) over(partition by Disaster_Subgroup, year)as Count_for_Disaster_Subgroup_per_year
from natural_disasters
order by 1
),Rankings as(
select year,Disaster_Subgroup,Count_for_Disaster_Subgroup_per_year,
dense_rank() over(partition by year order by Count_for_Disaster_Subgroup_per_year desc) as Ranking_for_subgroup_per_year
from distinct_subgroup
)
select * from Rankings;

-- The distnct disaster types 
select Disaster_Type,count(*) as Count_for_Disaster_Type from natural_disasters
group by 1
order by 2 desc;

-- The country with the most natural disasters 
select Country, count(*) from natural_disasters
group by country
order by 2 desc;

-- Finding the natural disaster that occurs the most in each country and ranking which disasters occur the most
with Country_of_natural_disaster as(
select distinct(Disaster_Type),country,
count(*) over(partition by country,Disaster_Type) as Count_for_Disaster_Type_per_Country
from natural_disasters
),ranking as(
select *,
dense_rank() over(partition by country order by Count_for_Disaster_Type_per_Country desc) as Rankings
from Country_of_natural_disaster
)
select * from ranking;

-- The top 15 distinct natural disaster names and how many times they occur 
select Event_Name, count(*) as Number_of_occurences from natural_disasters
where Event_Name is not null
group by Event_Name 
order by 2 desc
limit 15;

-- Finding out the regions most affected by natural disasters
select Region, count(Region)as Number_of_occurences_per_region from natural_disasters
group by Region
order by 2 desc;

-- Finding out with the highest total deaths associated with a named natural disaster
select Event_Name, Region,Total_Deaths  from natural_disasters
where Event_Name is not null and Total_Deaths is not null
order by Total_Deaths desc
Limit 1;

-- Finding out the top 3 natural disasters associated with names with the highest deaths per region
with Natural_disaster_by_region as(
select Event_Name, Region, Total_Deaths,
dense_rank() over(partition by Region order by Total_Deaths desc) as Ranking
from natural_disasters
where Event_Name is not null and Total_Deaths is not null 
)

select * from Natural_disaster_by_region 
where Ranking <=3;

-- Finding the natural disaster with the largest magnitude value per region 
with Event_magnitude as(
select distinct(Event_Name), Region, Dis_Mag_Scale, Dis_Mag_Value,
dense_rank() over(partition by Region order by  Dis_Mag_Value desc) as Ranking
from natural_disasters
where Event_Name is not null and Dis_Mag_Scale is not null and Dis_Mag_Value is not null 
)

select * from Event_magnitude where Ranking <=5;

-- Formatting the start and end dates and find out the longest natural disasters 
with Date_conversion as(
select Event_Name,Region,
concat(
	case
		when char_length(Start_Month) < 2 then concat(Start_Year,'-0',Start_Month)
		else concat(Start_Year,'-',Start_Month)
    end,
    '-',
	case
		when char_length(Start_Day) < 2 then concat('0',Start_Day)
        else Start_Day
	end
)as Formatted_start_date, Start_Year,Start_Month, Start_Day,
concat(
	case
		when char_length(End_Month) < 2 then concat(End_Year,'-0',End_Month)
		else concat(End_Year,'-',End_Month)
    end,
    '-',
	case
		when char_length(End_Day) < 2 then concat('0',End_Day)
        else End_Day
	end
)as Formatted_end_date, End_Year,End_Month, End_Day
from natural_disasters
where Start_Year is not null 
and Start_Month is not null 
and Start_day is not null
and End_Year is not null 
and End_Month is not null 
and End_day is not null
and Event_Name is not null 
)

select Event_Name,Region,Formatted_end_date,Formatted_start_date,
datediff(Formatted_end_date,Formatted_start_date)as Day_occurence from Date_conversion
order by Day_occurence desc;

-- Formatting the start and end dates and find out the longest natural disasters per region
with Date_conversion as(
select Event_Name,Region,
concat(
	case
		when char_length(Start_Month) < 2 then concat(Start_Year,'-0',Start_Month)
		else concat(Start_Year,'-',Start_Month)
    end,
    '-',
	case
		when char_length(Start_Day) < 2 then concat('0',Start_Day)
        else Start_Day
	end
)as Formatted_start_date, Start_Year,Start_Month, Start_Day,
concat(
	case
		when char_length(End_Month) < 2 then concat(End_Year,'-0',End_Month)
		else concat(End_Year,'-',End_Month)
    end,
    '-',
	case
		when char_length(End_Day) < 2 then concat('0',End_Day)
        else End_Day
	end
)as Formatted_end_date,
End_Year,End_Month, End_Day
from natural_disasters
where Start_Year is not null 
and Start_Month is not null 
and Start_day is not null
and End_Year is not null 
and End_Month is not null 
and End_day is not null
and Event_Name is not null 
),
Region_ranking as(
select Event_Name,Region,Formatted_end_date,Formatted_start_date,
datediff(Formatted_end_date,Formatted_start_date)as Day_occurence,
dense_rank() over(partition by Region order by datediff(Formatted_end_date,Formatted_start_date)desc) as Ranking
from Date_conversion
)
select * from Region_ranking where Ranking <= 5
