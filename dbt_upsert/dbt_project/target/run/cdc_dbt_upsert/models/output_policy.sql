
      
        
            delete from "cdc_db"."public"."output_policy"
            where (
                policy_id) in (
                select (policy_id)
                from "output_policy__dbt_tmp142844017154"
            );

        
    

    insert into "cdc_db"."public"."output_policy" ("policy_id", "policy_number", "status", "effective_date", "expiration_date", "holder_first_name", "holder_last_name", "holder_dob", "holder_email", "holder_phone", "holder_street", "holder_city", "holder_state", "holder_zip", "source_event_time")
    (
        select "policy_id", "policy_number", "status", "effective_date", "expiration_date", "holder_first_name", "holder_last_name", "holder_dob", "holder_email", "holder_phone", "holder_street", "holder_city", "holder_state", "holder_zip", "source_event_time"
        from "output_policy__dbt_tmp142844017154"
    )
  