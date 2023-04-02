# notionFDW
Simple PostgreSQL Foreign Data Wrapper for notion

This fdw uses [Multicorn](https://multicorn.org/)

### Installation
1. create multicorn extension `Create EXTENSION multicorn;`
2. create server and pass api key and ntion database id `
CREATE SERVER notion FOREIGN DATA WRAPPER multicorn
options (
  wrapper 'multicorn.mytest.NotionDataWrapper',
  api_key 'apikey',
  database_id 'database_id'
);`
3. create table `CREATE FOREIGN TABLE notion (
    id character varying,
    name character varying
) server notion;
`
