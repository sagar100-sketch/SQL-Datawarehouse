USE master;
GO
If Exists(select 1 from sys.databases where name='Datawarehouse')
Begin
    Alter Database Datawarehouse set SINGLE_USER with Rollback Immediate;
    DROP Database Datawarehouse;
END
GO
Create Database Datawarehouse;
GO
USE Datawarehouse;
GO

USE Datawarehouse;
GO
Create Schema bronze;
GO
Create Schema Silver;
GO
Create Schema Gold;
GO
