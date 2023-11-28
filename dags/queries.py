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

resource_allowcation = {
    'table': 'fact_resource_allowcation',
    'sql': '''
        select 
            a."ID" as raw_id,
            a."PROJECT_ID" as project_id,
            au.id as user_id,
            c.id as group_id,
            a."EFFORT_PERCENT" as effort_percent,
            date(a."FROM_DATE") as from_date,
            date(a."TO_DATE") as to_date
        from "AO_1F2264_FS_RES_ALLOCATION" a
        left join app_user au 
            on a."USER_KEY" = au.user_key
        inner join 
            (
                select "PROJECT_ID", "VALUE"
                from "AO_58D6C1_FP_FIELD_VALUE" 
                where "FIELD_ID" = 5
            )b
            on a."PROJECT_ID" = b."PROJECT_ID"
        left join cwd_group c
            on lower(c.group_name) = lower(b."VALUE")
        where au.id is not null and c.id is not null;
    '''
}

product_output = {
    'table': 'fact_product_output',
    'sql': '''
        SELECT 
            i.id as raw_id,
            p.id as project_id,
            cu.id as user_id,
            c.id as group_id,
            i.issuenum as issue_key,
            i.summary,
            issue_type.pname as issue_type,
            cfvstorypoints.numbervalue as story_point,
            cfvStartDate.datevalue as start_date,
            cfvEndDate.datevalue as end_date,
            date(complete.complete_date) as complete_date,
            status.pname as issue_status
        FROM jiraissue i
        JOIN app_user au ON i.assignee = au.user_key
        JOIN cwd_user cu ON cu.lower_user_name = au.lower_user_name
        JOIN issuestatus status on status.id = i.issuestatus
        JOIN (select * from issuetype where id in ('10001', '10002', '10103', '10003')) issue_type on issue_type.id = i.issuetype
        JOIN project p ON p.id = i.project
        LEFT JOIN customfieldvalue cfvStoryPoints ON i.id = cfvStoryPoints.issue and cfvStoryPoints.customfield = 10106
        LEFT JOIN customfieldvalue cfvEndDate ON i.id = cfvEndDate.issue and cfvEndDate.customfield = 10108
        LEFT JOIN customfieldvalue cfvStartDate ON i.id = cfvStartDate.issue and cfvStartDate.customfield = 10107
        LEFT JOIN issuelink issue_link ON i.id = issue_link.destination and issue_link.linktype = 10100
        LEFT JOIN (
            SELECT g.issueid, max(g.created) as complete_date
            FROM changeitem i
            INNER JOIN changegroup g on g.id = i.groupid
            WHERE i.field = 'status' and i.newvalue = '10001'
            GROUP BY g.issueid
        ) as complete ON complete.issueid = i.id
        LEFT JOIN (
            SELECT issue_type.pname as parent_type, temp_issue_link.source as parent, temp_issue_link.destination as chiren
            FROM jiraissue pi
            JOIN issuelink temp_issue_link ON pi.id = temp_issue_link.source and temp_issue_link.linktype = 10100
            JOIN issuetype issue_type on issue_type.id = pi.issuetype
            WHERE pi.issuetype in ('10001', '10002', '10103', '10003')
        ) as parent 
        ON parent.chiren = issue_link.destination
        inner join 
            (
                select "PROJECT_ID", "VALUE"
                from "AO_58D6C1_FP_FIELD_VALUE" 
                where "FIELD_ID" = 5
            )b
            on i.project = b."PROJECT_ID"
        left join cwd_group c
            on lower(c.group_name) = lower(b."VALUE")
        where au.id is not null and c.id is not null;
    '''
}

product_output_efficiency = {
    'table': 'fact_product_output_efficiency',
    'sql': '''
        SELECT 
            fra.project_id,
            fra.group_id,
            dp.project_name,
            dg.group_name,
            sum(fra.ra / 100) as ra,
            sum(fra.ra_standard / 100) as ra_standard,
            sum(fpo.done_mm) as done_mm,
            sum(fpo.done_mm) / sum(fra.ra / 100) as EEi,
            cast(max(fpo.complete_date / 100) as SIGNED) * 100 + 1 as date_id
        FROM fact_resource_allowcation fra 
        INNER JOIN fact_product_output fpo ON fra.project_id = fpo.project_id 
            AND fra.group_id = fpo.group_id 
            AND FORMAT(fpo.complete_date/100, 0) = FORMAT(fra.from_date/100, 0)
        inner join dim_project dp on dp.id = fra.project_id 
        inner join dim_group dg on dg.id = fra.group_id 
        WHERE fra.date = \'{date}\'
        GROUP BY fra.project_id, fra.group_id, FORMAT(fpo.complete_date/100, 0), FORMAT(fra.from_date/100, 0)
        HAVING ra > 0 and ra_standard > 0;
    '''
}

reource_utilization = {
    'table': 'fact_resource_utilization',
    'sql_data_mart': '''
        select
            fra.user_id,
            du.raw_id as raw_user_id,
            fra.group_id,
            dg.raw_id as raw_group_id,
            sum(ra/100) as ra,
            min(dd.id) as date_id,
            min(dd.`year_month`) as `year_month` 
        from fact_resource_allowcation fra
        inner join dim_date dd on dd.id = fra.from_date
        inner join dim_user du on du.id = fra.user_id 
        inner join dim_group dg on dg.id = fra.group_id 
        where fra.date = \'{date}\' 
        group by fra.user_id, fra.group_id, dd.`month`, dd.`year`;
    ''',
    'sql_jira': '''
        select 
            cu.id as user_id,
            cg.id as group_id,
            log.summary,
            cast(log.created as date) 
        FROM audit_log log 
        JOIN audit_item item 
            on log.id = item.log_id
        JOIN app_user au 
            on item.object_id = au.user_key
        JOIN cwd_user cu 
            on cu.lower_user_name = au.lower_user_name
        JOIN cwd_group cg
            on cg.group_name = log.object_name
        where log.summary in ('User added to group', 'User removed from group')
        and cg.group_name like 'BU%'
        ORDER BY
            cu.id,
            cg.id,
            log.created;
    '''
}