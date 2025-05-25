create or alter procedure bronze.load_data as
begin
	Declare @start_time DATETIME,@end_time DATETIME,@batch_start DATETIME,@batch_end DATETIME
	begin try
		set @batch_start=GETDATE();
		print 'Batch Loading Data...';
		print 'Load started at'+ cast(@batch_start as NVARCHAR);
		Truncate table bronze.crm_cust_info;
		BULK insert bronze.crm_cust_info
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			Tablock
		);
	
		Truncate table bronze.crm_prd_info;
		BULK insert bronze.crm_prd_info
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			Tablock
		);

		Truncate table bronze.crm_sales_details;
		BULK insert bronze.crm_sales_details
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			Tablock
		);


		Truncate table bronze.erp_loc_a101;
		BULK insert bronze.erp_loc_a101
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		Truncate table bronze.erp_loc_a101;
		BULK insert bronze.erp_loc_a101
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
	
		Truncate table bronze.erp_cust_az12;
		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			First_row=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		Truncate table bronze.erp_px_cat_g1v2;
		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\LENOVO\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			First_row=2,
			FIElDTERMINATOR=',',
			TABLOCK
		);
		set @batch_end=GETDATE();
		print 'Load completed at '+cast(@batch_end as NVARCHAR);
		print 'Total Batch processing time is '+Cast(DATEDIFF(second,@batch_end,@batch_start)as NVARCHAR)+ ' seconds.'
	END TRY
	BEGIN CATCH
		print 'Batch load Failed...';
		print 'The error message is '+ ERROR_MESSAGE();
	END CATCH
END


EXEC bronze.load_data;
