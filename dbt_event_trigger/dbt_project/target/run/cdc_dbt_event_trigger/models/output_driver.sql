
      
        
            delete from "cdc_db"."public"."output_driver"
            using "output_driver__dbt_tmp151104049839"
            where (
                
                    "output_driver__dbt_tmp151104049839".policy_id = "cdc_db"."public"."output_driver".policy_id
                    and 
                
                    "output_driver__dbt_tmp151104049839".vehicle_vin = "cdc_db"."public"."output_driver".vehicle_vin
                    and 
                
                    "output_driver__dbt_tmp151104049839".license_number = "cdc_db"."public"."output_driver".license_number
                    
                
                
            );
        
    

    insert into "cdc_db"."public"."output_driver" ("policy_id", "vehicle_vin", "driver_name", "license_number", "is_primary")
    (
        select "policy_id", "vehicle_vin", "driver_name", "license_number", "is_primary"
        from "output_driver__dbt_tmp151104049839"
    )
  