# WARNING
Api-key authentication and formatting for fetch responses has not been set as of yet. If you plan to use these features, beware of possible setbacks.


## Routes
Keep in mind \[Anything in braces is optional].
Defaults can be changed in config.rs

For any route, a database keyspace can be appended to the path. Availible options are `valid`, `discord`, and `email`. Defaults to email.


### GET /fetch/\[KEYSPACE]/\[NUM]
Fetches email(s) from database. NUM is the number of emails to fetch, defaults to 1000.

### POST /add/\[KEYSPACE]
Adds email(s) to database.

Data posted to this location must be formatted as such:  
// TODO - add example json

### POST /invalidate/\[KEYSPACE]
Deletes email with given id from db

Data posted to this location must be formatted as such:  
```json
{
    "uuid": ["uuids", "as", "strings", "go", "here"]
}
```


## Build commands
Relevant dependancies for this package are
* libssl-dev

Install using the following script
```sh
sudo apt install libssl-dev

```