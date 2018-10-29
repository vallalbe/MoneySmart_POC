select exper as Experiment,grp as GroupName,count( distinct usr) Total_Users from log_table group by exper,grp order by exper,grp,Total_Users;


SELECT 
    exper,
    dte,
    MAX(DISTINCT user_group_assignments) AS highest_user_group_assignments
FROM
    (SELECT 
        DATE(dt) dte, exper, COUNT(usr) user_group_assignments
    FROM
        log_table
    GROUP BY exper , dte
    ORDER BY exper , dte , user_group_assignments) AS x
GROUP BY exper , dte;

              
              
SELECT dte as Day,exper as Experiment, highest_user_group_assignments as Highest_number_of_user_group_assignments
FROM   q2TmpDF s1
WHERE  highest_user_group_assignments=(SELECT MAX(s2.highest_user_group_assignments)
              FROM q2TmpDF s2
              WHERE s1.exper = s2.exper)


