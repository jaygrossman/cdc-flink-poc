
      
        
            delete from "cdc_db"."public"."output_coverage"
            using "output_coverage__dbt_tmp151103889606"
            where (
                
                    "output_coverage__dbt_tmp151103889606".policy_id = "cdc_db"."public"."output_coverage".policy_id
                    and 
                
                    "output_coverage__dbt_tmp151103889606".coverage_type = "cdc_db"."public"."output_coverage".coverage_type
                    
                
                
            );
        
    

    insert into "cdc_db"."public"."output_coverage" ("policy_id", "coverage_type", "coverage_limit", "deductible", "premium")
    (
        select "policy_id", "coverage_type", "coverage_limit", "deductible", "premium"
        from "output_coverage__dbt_tmp151103889606"
    )
  