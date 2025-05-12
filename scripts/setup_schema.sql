/*
=============================================================
Create Schemas
=============================================================
Script Purpose:
    This script sets up three schemas ('bronze', 'silver', and 'gold') freshly by deleting them if already exist within the database.
	
WARNING:
    Running this script will drop all the database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
Manual steps:
    Create the database manually from pgadmin, cli. Because postgres do not all to switch between database using script.
*/
DROP SCHEMA IF EXISTS bronze CASCADE;
CREATE SCHEMA bronze;
DROP SCHEMA IF EXISTS silver CASCADE;
CREATE SCHEMA silver;
DROP SCHEMA IF EXISTS gold CASCADE;
CREATE SCHEMA gold;
