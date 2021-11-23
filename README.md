# FLink

Content:

	1. Dockerfile -	Setup for python container
	2. docker-compose.yml - Containers configuration
	3. init.sql - SQL statement to create database and empty table.
	4. requirements.txt - Python libraries to install
	5. Python/
		5.1 202106_flink_data_engieering_sample_data.json - input file
		5.2 data_processing.py - script solution

How to use it:

	1. Clone the repository to your local machine.
	2. Be sure to have docker installed.
	3. Move to /Flink/Container/ folder.
	4. Execute the following commands that will create two containers.
		docker-compose up -d
	5. Get a bash shell for both containers	
		a) docker-compose exec mysql_db bash
		b) docker-compose exec python_app bash
	6. The first one is the database container, execute the following command to login to the DB:
		mysql -u root -h mysql_db -p
		*the password is: password
	7. Now you should be able to list the 'flink_2021' database that has one empty table 'user_events'.
	8. Move to the python container bash and execute the following command:
		python data_processing.py
	9. Go back to the database container and see how many records were written. 


Esaú Reyes
