group = {
    'table': 'dim_group',
    'sql': '''
        select 
            id, 
            group_name, 
            active, 
            created_date, 
            updated_date 
        from cwd_group;
    '''
}

user = {
    'table': 'dim_user',
    'sql': '''
        select 
            id,
            user_key,
            display_name,
            lower_display_name,
            email_address,
            active,
            created_date,
            updated_date
        from jirauser;
    '''
}

project = {
    'table': 'dim_project',
    'sql': '''
        select 
        p.id,
        p.pname as project_name,
        p."lead" as "lead",
        p.description,
        p.pkey as project_key,
        case when b."FIELD_ID" = 5 then 1 else 0 end as project_status,
        p.projecttype as project_type,
        a."CREATED_TIME" as create_time,
        a."UPDATED_TIME" as update_time
    from project p
    left join "AO_58D6C1_FP_PROJECT_CONFIG" a
        on p.id = a."PROJECT_ID"
    left join "AO_58D6C1_FP_FIELD_VALUE" b
        on p.id = b."PROJECT_ID";
    '''
}