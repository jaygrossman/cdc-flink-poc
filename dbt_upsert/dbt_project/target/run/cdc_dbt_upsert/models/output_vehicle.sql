
      
        
            delete from "cdc_db"."public"."output_vehicle"
            using "output_vehicle__dbt_tmp142844157564"
            where (
                
                    "output_vehicle__dbt_tmp142844157564".policy_id = "cdc_db"."public"."output_vehicle".policy_id
                    and 
                
                    "output_vehicle__dbt_tmp142844157564".vin = "cdc_db"."public"."output_vehicle".vin
                    
                
                
            );
        
    

    insert into "cdc_db"."public"."output_vehicle" ("policy_id", "vin", "year_made", "make", "model")
    (
        select "policy_id", "vin", "year_made", "make", "model"
        from "output_vehicle__dbt_tmp142844157564"
    )
  