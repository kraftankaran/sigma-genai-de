
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select failure_rate_pct
from SIGMA_DE.PUBLIC.mart_merchant_performance
where failure_rate_pct is null



  
  
      
    ) dbt_internal_test