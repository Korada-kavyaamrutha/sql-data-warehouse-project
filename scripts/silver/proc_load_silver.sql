/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
 DECLARE @start_time DATETIME, @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME; 
	 SET @batch_start_time = GETDATE();
     Begin TRY

			PRINT '=======================================';
			PRINT 'Loading Silver layer';
			PRINT '=======================================';
			PRINT '---------------------------------------';
			PRINT 'Loading CRM Tables into silver layer ';
			PRINT '---------------------------------------';
			    --Cust_info table
				SET @start_time = GETDATE();
					Print '>> Truncating Table : silver.crm_cust_info';
					TRUNCATE TABLE silver.crm_cust_info;
					PRINT '>> Inserting Data Into: silver.crm_cust_info';
					INSERT INTO silver.crm_cust_info(
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date)

					select 
							cst_id,
							cst_key,
							TRIM(cst_firstname) as cst_firstname, --removing unwanted spaces to ensure data consistency
							TRIM(cst_lastname) as cst_lastname,
							CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
								 when UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
								 else 'n/a'
								 END cst_marital_status, --Normalize marital status values to readable format
							CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
								 when UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
								 else 'n/a' --Normalize gender values to readable format
								 END cst_gndr,
							cst_create_date
					from(
							select *,
							ROW_number() over (partition by cst_id order by cst_create_date DESC) as flag_last
							from bronze.crm_cust_info
							where cst_id IS NOT NULL-- removing null values
					) t where flag_last = 1 -- select the most recent record per customer
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			  --select * from silver.crm_cust_info

			  ---prd_info
			  SET @start_time = GETDATE();
					Print '>> Truncating Table :silver.crm_prd_info';
					TRUNCATE TABLE silver.crm_prd_info;
					PRINT '>> Inserting Data Into: silver.crm_prd_info';
					  INSERT INTO silver.crm_prd_info(
					  prd_id,
					  cat_id,
					  prd_key,
					  prd_nm,
					  prd_cost,
					  prd_line,
					  prd_start_dt,
					  prd_end_dt
					  )
					  select 
					  prd_id,
					  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- extract category ID
					  SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key, -- Extract Product key
					  prd_nm,
					  ISNULL(prd_cost, 0) AS prd_cost, -- null handling
					   CASE UPPER(TRIM(prd_line))
						   WHEN 'M' THEN 'Mountain'
						   WHEN 'R' THEN 'ROAD'
						   WHEN 's' THEN 'other sales'
						   WHEN 'T' THEN 'Touring'
						   else 'n/a'
						   END AS prd_line,--Map product line codes to descriptive values
					  CAST(prd_start_dt as DATE) AS prd_start_dt,
					  CAST(LEAD(prd_start_dt) over (partition By prd_key order by prd_start_dt)-1 as DATE) AS prd_end_dt -- calculate end date as one day before thenext start date
					  from bronze.crm_prd_info
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			 --select * from silver.crm_prd_info


			 --sales-details
			 SET @start_time = GETDATE();
					 Print '>> Truncating Table :silver.crm_sales_details';
					 TRUNCATE TABLE silver.crm_sales_details;
					 PRINT '>> Inserting Data Into: silver.crm_sales_details';
					 INSERT INTO Silver.crm_sales_details(
							sls_ord_num,
							sls_prd_key,
							sls_cust_id,
							sls_order_dt,
							sls_ship_dt,
							sls_due_dt,
							sls_sales,
							sls_quantity,
							sls_price
							)
					select 
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					case when sls_order_dt = 0 or LEN(sls_order_dt) !=8 then null
						 else cast(cast(sls_order_dt AS varchar) as Date)
						 end AS sls_order_dt,
					case when sls_ship_dt = 0 or LEN(sls_ship_dt) !=8 then null
						 else cast(cast(sls_ship_dt AS varchar) as Date)
						 end AS sls_ship_dt,
					case when sls_due_dt = 0 or LEN(sls_due_dt) !=8 then null
						 else cast(cast(sls_due_dt AS varchar) as Date)
						 end AS sls_due_dt,
					CASE WHEN sls_sales IS NULL or sls_sales !=sls_quantity * abs(sls_price)
						then sls_quantity * ABS(sls_price)
						ELSE sls_sales
						end as sls_sales ,
					sls_quantity,
					case when sls_price is null or sls_price <=0
						 then sls_sales / NULLIF(sls_quantity,0)
						 else sls_price
						 end as sls_price
					from bronze.crm_sales_details
				SET @end_time = GETDATE();
				PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			--select* from silver.crm_sales_details

			PRINT '---------------------------------------';
			PRINT 'Loading ERP Tables into silver layer';
			PRINT '---------------------------------------';
				
				
			--erp cust AZ12
			SET @start_time = GETDATE();
					Print '>> Truncating Table :silver.erp_cust_az12';
					TRUNCATE TABLE silver.erp_cust_az12;
					PRINT '>> Inserting Data Into: silver.erp_cust_az12';
					INSERT INTO silver.erp_cust_az12(
					cid,
					bdate,
					gen)
					select 
					CASE WHEN cid LIKE 'NAS%' then SUBSTRING(cid, 4, len(cid)) --removing 'NAS' prefix if present 
						 ELSE cid
						 END as cid,
					CASE WHEN bdate > GETDATE() THEN NULL
						ELSE bdate
					END AS bdate, --set future birthdates to NULL
					CASE WHEN UPPER(TRIM(gen)) IN ( 'F', 'Female') THEN 'Female'
						 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
						 else 'n/a'
						 END as gen -- normalize gender values ans handle unknown cases
					from bronze.erp_CUST_AZ12
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			--select * from silver.erp_cust_az12


			--erp_loc_a101
			SET @start_time = GETDATE();
					Print '>> Truncating Table :silver.erp_loc_a101';
					TRUNCATE TABLE silver.erp_loc_a101;
					PRINT '>> Inserting Data Into: silver.erp_loc_a101';
					INSERT INTO silver.erp_loc_a101(
					cid,
					cntry)
					select 
							REPLACE(cid, '-', '') cid,--handling invalid values '-'
							CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
								 when TRIM(cntry) IN ('US', 'USA') THEN 'United states'
								 when TRIM(cntry) = '' OR cntry iS NULL THEN 'n/a'
								 ELSE TRIM(cntry)
								  END AS cntrya -- normalize and handle missing or blank country codes
					from bronze.erp_loc_a101;
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

			--select * from  silver.erp_loc_a101

			--erp_px_cat_g1v2
			SET @start_time = GETDATE();
					Print '>> Truncating Table :silver.erp_PX_CAT_G1V2';
					TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
					PRINT '>> Inserting Data Into: silver.erp_PX_CAT_G1V2';
					Insert INTO silver.erp_PX_CAT_G1V2(
					id,
					cat,
					subcat,
					maintenance)
					select 
					id,
					cat,
					subcat,
					maintenance
					from bronze.erp_PX_CAT_G1V2
             SET @end_time = GETDATE();
			 PRINT '>> Load Duration: ' +CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			--select * from silver.erp_PX_CAT_G1V2

				Set @batch_end_time = GETDATE();
				PRINT '-----------------------------------------'
				PRINT 'Loading Silver layer is completed';
			    PRINT ' - Total Load Duration: ' +CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
				PRINT '-----------------------------------------'
	END TRY
	Begin catch 
		PRINT '============================================';
		PRINT 'ERROE OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_State() AS NVARCHAR);
	    PRINT '============================================';

	END catch

END
