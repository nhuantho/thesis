group = {
    'table': 'dim_group',
    'sql': '''
        select 
            id,
            group_name,
            active,
            created_date,
            deleted_date,
            updated_date,
            description
        from jira_group
        where updated_date >= {start_date} and updated_date < {end_date};
    '''
}

user = {
    'table': 'dim_user',
    'sql': '''
        select
            id,
            user_key,
            user_name,
            display_name,
            active,
            created_date,
            deleted_date,
            updated_date,
            email_address,
            group_id 
        from jira_user
        where updated_date >= {start_date} and updated_date < {end_date};
    '''
}

project = {
    'table': 'dim_project',
    'sql': '''
        select 
            id,
            pname as project_name,
            user_lead,
            description,
            pkey as project_key,
            projecttype as project_type,
            group_id,
            created as created_date,
            updated as updated_date,
            completed as completed_date,
            is_completed
        from jira_project
        where updated >= {start_date} and updated < {end_date};
    '''
}

fact_workforce_productivity = {
    'table': 'fact_workforce_productivity',
    'sql': '''
        select 
            mm.project_id,
            mm.user_id,
            mm.time_id as date_id,
            mm.mm,
            ra.ra
        from 
        (
            select 
                project_id,
                assignee as user_id,
                sum(story_point) as mm,
                to_char(duedate,'YYYY-MM') as time_id
            from jira_issue
            where lower(issuetype) != 'sub-task' 
                and duedate >= {start_date} and duedate < {end_date}
            group by project_id, assignee, to_char(duedate,'YYYY-MM')
        ) mm
        inner join
        (
            select 
                project_id,
                user_id,
                sum(effort_precent)/count(*) as ra,
                to_char("date",'YYYY-MM') as time_id
            from jira_resource_allowcation
            where "date" >= {start_date} and "date" < {end_date}
            group by project_id, user_id, to_char("date",'YYYY-MM')
        ) ra on ra.project_id = mm.project_id 
            and ra.user_id = mm.user_id
            and ra.time_id = mm.time_id;
    '''
}

fact_product_output_efficiency = {
    'table': 'fact_product_output_efficiency',
    'sql': '''
        select 
            mm.project_id,
            jp.group_id,
            mm.time_id as date_id,
            mm.mm,
            ra.ra
        from 
        (
            select 
                project_id,
                sum(story_point) as mm,
                to_char(duedate,'YYYY-MM') as time_id
            from jira_issue
            where lower(issuetype) != 'sub-task'
                and duedate >= {start_date} and duedate < {end_date}
            group by project_id, to_char(duedate,'YYYY-MM')
        ) mm
        inner join
        (
            select 
                project_id,
                sum(effort_precent)/count(*) as ra,
                to_char("date",'YYYY-MM') as time_id
            from jira_resource_allowcation
            where "date" >= {start_date} and "date" < {end_date}
            group by project_id, to_char("date",'YYYY-MM')
        ) ra on ra.project_id = mm.project_id 
            and ra.time_id = mm.time_id
        inner join jira_project jp on jp.id = mm.project_id;
    '''
}

# load full
group_full = {
    'table': 'dim_group',
    'sql': '''
        select 
            id,
            group_name,
            active,
            created_date,
            deleted_date,
            updated_date,
            description
        from jira_group;
    '''
}

user_full = {
    'table': 'dim_user',
    'sql': '''
        select
            id,
            user_key,
            user_name,
            display_name,
            active,
            created_date,
            deleted_date,
            updated_date,
            email_address,
            group_id 
        from jira_user;
    '''
}

project_full = {
    'table': 'dim_project',
    'sql': '''
        select 
            id,
            pname as project_name,
            user_lead,
            description,
            pkey as project_key,
            projecttype as project_type,
            group_id,
            created as created_date,
            updated as updated_date,
            completed as completed_date,
            is_completed
        from jira_project;
    '''
}

fact_workforce_productivity_full = {
    'table': 'fact_workforce_productivity',
    'sql': '''
        select 
            mm.project_id,
            mm.user_id,
            mm.time_id as date_id,
            mm.mm,
            ra.ra
        from 
        (
            select 
                project_id,
                assignee as user_id,
                sum(story_point) as mm,
                to_char(duedate,'YYYY-MM') as time_id
            from jira_issue
            where lower(issuetype) != 'sub-task' and duedate < '2023-11-30'
            group by project_id, assignee, to_char(duedate,'YYYY-MM')
        ) mm
        inner join
        (
            select 
                project_id,
                user_id,
                sum(effort_precent)/count(*) as ra,
                to_char("date",'YYYY-MM') as time_id
            from jira_resource_allowcation
            where "date" < '2023-12-01'
            group by project_id, user_id, to_char("date",'YYYY-MM')
        ) ra on ra.project_id = mm.project_id 
            and ra.user_id = mm.user_id
            and ra.time_id = mm.time_id;
    '''
}

fact_product_output_efficiency_full = {
    'table': 'fact_product_output_efficiency',
    'sql': '''
        select 
            mm.project_id,
            jp.group_id,
            mm.time_id as date_id,
            mm.mm,
            ra.ra
        from 
        (
            select 
                project_id,
                sum(story_point) as mm,
                to_char(duedate,'YYYY-MM') as time_id
            from jira_issue
            where lower(issuetype) != 'sub-task' and duedate < '2023-11-30'
            group by project_id, to_char(duedate,'YYYY-MM')
        ) mm
        inner join
        (
            select 
                project_id,
                sum(effort_precent)/count(*) as ra,
                to_char("date",'YYYY-MM') as time_id
            from jira_resource_allowcation
            where "date" < '2023-12-01'
            group by project_id, to_char("date",'YYYY-MM')
        ) ra on ra.project_id = mm.project_id 
            and ra.time_id = mm.time_id
        inner join jira_project jp on jp.id = mm.project_id;
    '''
}
