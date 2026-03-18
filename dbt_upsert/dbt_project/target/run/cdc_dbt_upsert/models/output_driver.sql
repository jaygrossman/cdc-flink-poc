
      
        
            delete from "cdc_db"."public"."output_driver"
            using "output_driver__dbt_tmp142843870814"
            where (
                
                    "output_driver__dbt_tmp142843870814".policy_id = "cdc_db"."public"."output_driver".policy_id
                    and 
                
                    "output_driver__dbt_tmp142843870814".vehicle_vin = "cdc_db"."public"."output_driver".vehicle_vin
                    and 
                
                    "output_driver__dbt_tmp142843870814".license_number = "cdc_db"."public"."output_driver".license_number
                    
                
                
            );
        
    

    insert into "cdc_db"."public"."output_driver" ("policy_id", "vehicle_vin", "driver_name", "license_number", "is_primary")
    (
        select "policy_id", "vehicle_vin", "driver_name", "license_number", "is_primary"
        from "output_driver__dbt_tmp142843870814"
    )
  