# DSpace-CRIS Loader
##Definition

This loader has born under the requirement of populating a DSpace-CRIS project from zero at CSUC.

Out of the box DSpace-CRIS provides manual ways through DSpace login system for inserting data as an administrator or researcher account, both cases ends in a "fill the gaps" interface where individuals complete his own data, so, is not feasible for loading the actual data present in another source.

Understanding how and where the data is stored in DSpace-CRIS is crucial for making a massive loader for the system so:
###How?

>DSpace-CRIS uses Hibernate framework in order to convert a database tables in Java objects.
So we assume that every time we insert new data using the web interface some Java objects are setted with this info and Hibernate triggers some queries into the database to store the info.
This changes are also updated in the SolR search engine.

###Where?

>CRIS and also DSpace data is stored in a Postgres/Oracle database depending of the installation. So it's here where we want to attack in order to load our data.
Also SolR engine contains a "copy" of this data which is provided from the Postgres/Oracle database, so once the database has our data SolR should be fine for a reindex to be ready to work.


So basically what we have done is code some Java classes that provide an interface to easily prepare a "connector" to your actual source data in order to get it loaded into DSpace-CRIS.

##Requirements

- DSpace-CRIS version 4.1.2 or greater
- Have access to the database

##Disclaimer

- Only tested in Postgres setup
- Needs a SolR re-index after this loader finishes

##Technology 

We have used PostgreSQL JDBC driver, which is supposed to be the fastest driver to insert data in Postgres.

##Detail about DSpace-CRIS database ID's

As we mentioned previously, DSpace-CRIS uses Hibernate, this shouldn't be a problem but is not the case.

All (or almost all) the extra tables that belong to CRIS extension have a sequencer to give new id's to every new row, this sequencer can be easily view from postgres, for example imagine you want to view the sequencer of the jdyna_values table
```sql
dspace=# select * from jdyna_values_seq;
  sequence_name   | last_value | start_value | increment_by |      max_value      | min_value | cache_value | log_cnt | is_cycled | is_called 
------------------+------------+-------------+--------------+---------------------+-----------+-------------+---------+-----------+-----------
 jdyna_values_seq |       3216 |           1 |            1 | 9223372036854775807 |         1 |           1 |       0 | f         | f
(1 row)
```
Here we can see right now that last_value delivered has been 3216. If we look inside the jdyna_values table and search for the biggest id it's supposed to return 3216 because it's what the sequencer had said previously, so, let's check it.
```sql
select max(id) from jdyna_values;
  max   
--------
 160802
(1 row)
```
We can see that something doesn't match, the sequencer says that last id delivered is 3216 but we can clearly see that there are bigger id's stored, what's happening?

When we are creating CRIS entities through the web interface (researchers, projects, org units, etc...) the value of the fields we are introducing goes directly as a new row in jdyna_values, the id of every single row of this table is assigned by Hibernate not the sequencer itself!, so... what's the point of having a sequencer?

The sequencer has his role but not in the way as we expect, what Hibernate does is use the sequencer as a kind of base counter where an offset will be applied, in detail what it does is for every sequencer unit, Hibernate will delivery 50 id's, so, if we have the sequencer with value 1, Hibernate will give id's from 1 to 50, when id 51 arrives the sequencer will be updated in 1 unit more resulting in this case in 2, so now is prepared for giving id from 51 to 100 and etc...    

We saw that we have the sequencer with the value 3216, if we make the "offset operation" we got 3216x50=160800, mainly the same as the biggest id has we got from the table.

The 2 unit difference it's because Hibernate it's inside the 50 offset margin over the value 3216.

More info about this issue searching by allocationSize in Hibernate documentation.
###How this affects us?

The problem is that we are inserting new entities (researchers, projects, org units, etc...) massively directly by Postgres, ergo this means that we are doing job parallel to Hibernate.

We need a synchronization mechanism in order to Hibernate cannot notice what we are doing.

We have to consider 2 things:
####1. How to obtain a new id when I am directly inserting things into Postgres?
I will start explaining the case where we will encounter the problem. Continuing with the last example, if we take the last_value of the sequencer and multiply it by 50 we get 160800, so we will use the id 160800 for our new row... ERROR, we know looking at the jdyna_values table that the biggest id is 160802 so we surely will obtain a nice "duplicated primary key" error when doing the insert.

The solution will be:

- Obtain the next id according the sequencer: 3216x50=160800, 160800+1=160801
- Obtain the next id according the biggest value of the table: 160802+1 = 160803
- Get the biggest one between point 1 and 2 (160803)


####2. How to ensure that the next id that Hibernate delivers won't be already in use?
As is possible to insert entities from web and from database, we have to ensure that where we doesn't have control, that is to say Hibernate (it's the same to say by the web), never got the possibility to deliver and id that is begin used.
The solution here is a little bit nasty, we can keep the sequencer updated (using the SQL command ALTER) when we insert things directly into Postgres, but what we cannot do (or we don't know the way) is notify that Hibernate check the base value of the sequencer that's begin modified using ALTER, the only way to make Hibernate refresh the base value of the sequencer is restarting Tomcat.
Summarizing, the solution is:
- Inserting entities in Postgres assigning id's in the way described previously
- At the same time, update the sequencers (when they go over the 50 units offset), for example:
```sql
ALTER SEQUENCE jdyna_values_seq RESTART WITH 3350
```
- Once your import process finishes, stop Tomcat
- Start up Tomcat

A big warning is need to be mentioned here, there's no solution nowadays for working inserting things through web and at the same time directly into Postgres.

Note that this procedure applies to all the DSpace-CRIS tables that have sequencer.

##Usage

This loader is delivered in Eclipse project format, that's because it's not a final product as it's missing the code that connects to your data source stream.

Except the example package that can be run directly.

##Code
```
Packages

example
  |
  |--- MainExample.java //insert a researcher
  |--- RelationExample.java //insert a project, org unit and a researcher linked between them
  |--- UpdateExample.java //update the fields of a project

loader
  |
  |--- Attribute.java //field wrapper for CRIS entities
  |--- JSONlogger.java //this class generate a final report
  |--- Loader.java //the main class

postgresql
  |
  |--- DBconnector.java //this creates a database connection
  |--- DButils.java //general functions for doing things with the CRIS data
  |--- SQLsentences.java //list of all of SQL queries that use this software
```

##Examples

This examples are supposed to be tested in a clean DSpace-CRIS database and have loaded previously the initial dspace-cris configuration located in /etc/postgres/base-configuration-crismodule.sql. If you need to clean your database in any moment during this tests you can run this SQL sentences (order is important):
```sql
delete from cris_rp_prop;
delete from cris_do_prop;
delete from cris_ou_prop;
delete from cris_pj_prop;
delete from jdyna_values;
delete from cris_rpage;
delete from cris_project;
delete from cris_orgunit;
delete from cris_do;
```
###MainExample.java

This is the basic and first example to begin with.

What this example does is add a researcher to the system.

We can see in first place the instantiation of the class Loader which will be using for manage our CRIS entities, it needs the location of the database including his name, user and password.
```java
Loader CRISloader = new Loader(databaseUrl,user,password,null); //instantiate the CRIS loader
```

Once this, we set up our researcher data, every field of the researcher is wrapped in an Attribute class, depending the type of field we want to add we need to pass different things, here is a chart:

| Field type    | Syntax        | 
| ------------- |:-------------:| 
| TEXT	| java new Attribute(String shortname, String value, Dtype.TEXT) |
| LINK	| new Attribute(String shortname, String linkDescription, String linkValue) |
| DATE	| new Attribute(String shortname, String date, Dtype.DATE) |
| RPPOINTER	| new Attribute(String shortname, String rpid, Dtype.RPPOINTER) |
| PJPOINTER	| new Attribute(String shortname, String pjid, Dtype.PJPOINTER) |
| OUPOINTER	| new Attribute(String shortname, String ouid, Dtype.OUPOINTER) |
| DOPOINTER	| new Attribute(String shortname, String doid, Dtype.DOPOINTER) |


You can see the shortname of every field in the administrator of your DSpace-CRIS: 

![Alt text](/readme/shortname.png?raw=true)

After that we can run the *addResearcher* function in order to finally insert the researcher into the system, setting also a unique source ID and source reference.

If all finishes successfully, we can re-index DSpace in order to see this new researcher.


###RelationExample.java

What this example does is add a researcher, a project and a organization unit to the system.

The project and the organization unit are inserted in the same way we do with the researcher in the previous example, fill the fields and ready to go. The important here is how to manage the relation between the project/organization unit and the researcher.

Here is a representation of what we are going to do:


![Alt text](/readme/relationExample.png?raw=true)


In order to link the researcher with the project as principal investigator, we need to add a field of type RPPOINTER (Researcher Page Pointer) in the project, this field needs also the RPID (internal ID that DSpace-CRIS generates automatically for every CRIS entity) which can be obtained using the function *getRPidBySourceidANDsourceref* and passing to it the source ID and source reference of the researcher we want to link.

We do the same with the organization unit.

Remember to reindex to see the changes.


###UpdateExample.java

We will divide this example in two parts, the first part consists in adding a new project, as we do in previous examples, nothing to comment here.

The second part tries to update some fields of this project.

Here's an easy diagram of what we are inserting after running this example:


![Alt text](/readme/updateExample.png?raw=true)

So basically for updating a CRIS entity already found in the system we need to create a new one, with the new/updated fields as we do when inserting a new one but calling a different function, for example in this case we are updating a project so instead of calling *addProject* we are going to call *updateProject* passing the PJID of the actual project in the system.

Important behaviour is need to be explained here. What happens if the two field we are trying to update have the same value? or different? here is a result table:

|new vs old values	| repeatable field? |	result |
| ----------------- |:-----------------:| -------|
|different | no |	old changed by new |
|equal | no	| no changes |
|different |	yes |	new added and old conserved |
|equal | yes |	no changes |


We can also go further and change the "equal" or "different" conditions setting this on the Attribute class.

In this example we can see how the start date of the new project is 2015-01-15 and we want to update it but **ONLY IF** the new date is bigger to actual one, so in the update project we set the Attribute object like:

```java 
Attribute startDate = new Attribute("startdate","2016-01-15",Dtype.DATE);
startDate.setUpdateCondition(">");
```
Which would change the value only if new one is bigger (>).

You can use > < >= or <= in order to fit your needs.
