
      
        
            delete from "cdc_db"."public"."output_claim"
            using "output_claim__dbt_tmp151103704267"
            where (
                
                    "output_claim__dbt_tmp151103704267".policy_id = "cdc_db"."public"."output_claim".policy_id
                    and 
                
                    "output_claim__dbt_tmp151103704267".claim_id = "cdc_db"."public"."output_claim".claim_id
                    
                
                
            );
        
    

    insert into "cdc_db"."public"."output_claim" ("policy_id", "claim_id", "claim_date", "amount", "status", "description")
    (
        select "policy_id", "claim_id", "claim_date", "amount", "status", "description"
        from "output_claim__dbt_tmp151103704267"
    )
  