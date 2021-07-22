import json
import os

from helpers import query, log, sparqlQuery, sparqlUpdate
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_int, sparql_escape_datetime
from string import Template

from time import sleep


def get_job(task):
    """
    Get the first queued job in the triple store
    :return: job description with source file location, task identifier and job id
    """
    q = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/ext/>
    SELECT ?source ?task ?uuid
    FROM <http://mu.semte.ch/application> 
    WHERE {
       ?job a <http://mu.semte.ch/vocabularies/ext/Job> ;
          mu:source ?source;
          mu:task $task;
          mu:uuid ?uuid;
          mu:status "queued" . 
    }
    LIMIT 1
    """).substitute(
        task=sparql_escape_string(task),
    )

    res = my_query(q)

    r = list(res["results"]["bindings"])

    if r:
        return r[0]
    else:
        return None


def update_job(id, status):
    """
    Forward status of job to the next state. If a state is skipped, a new job will be created!
    :param id: job id
    :param status: new status (queued, processing, done)
    :return:
    """
    stat_list = ["queued", "processing", "done"]
    new_idx = stat_list.index(status)

    q = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/ext/>
    WITH <http://mu.semte.ch/application>
    DELETE {?job mu:status $oldstate}
    INSERT {?job mu:status $newstate}
    WHERE {
        ?job a <http://mu.semte.ch/vocabularies/ext/Job> ;
          mu:uuid "$uuid" .
    }
    """).substitute(
        uuid=id,
        oldstate=sparql_escape_string(stat_list[new_idx - 1]),
        newstate=sparql_escape_string(status),
    )
    my_update(q)


def construct_get_file_by_id(file_id):
    """
    Construct query to get file based on file id
    :param file_id: string:file id
    :return: string:query
    """
    query_template = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
    PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
    SELECT (?phys_file AS ?uri)
    WHERE {
        GRAPH $graph {
            ?virt_file a nfo:FileDataObject ;
                mu:uuid $uuid .
            ?phys_file a nfo:FileDataObject ;
                nie:dataSource ?virt_file .
        }
    }
    LIMIT 1
    """)
    return query_template.substitute(
        graph=sparql_escape_uri("http://mu.semte.ch/application"),
        uuid=sparql_escape_string(file_id))


def get_file_by_id(id):
    """
    Execute query get file by id
    :param id: sting:file id
    :return: file information
    """
    return my_query(construct_get_file_by_id(id))


def start_loop(call_method):
    """
    Run the job loop to fetch jobs and execute them is correct task type.
    Single threaded: only one task is executed per loop
    :param call_method: Method to be called on action
    :return:
    """
    watchedTask = os.environ.get('TASK')

    while True:
        job = get_job(watchedTask)
        if job:
            id = job["uuid"]["value"]
            file_id = job["source"]["value"]

            log(job)

            try:
                update_job(id, "processing")
                file = get_file_by_id(file_id)
                uri = file["results"]["bindings"][0]["uri"]["value"].replace("share://", "/share/")
                log(uri)
                with open(uri) as f:
                    data = f.read()
                data = json.loads(data)

                resp = call_method(data)
                log(resp)

            except Exception as e:
                log(e)

            update_job(id, "done")
        sleep(20)


def my_query(the_query):
    """Execute the given SPARQL query (select/ask/construct)on the tripple store and returns the results
    in the given returnFormat (JSON by default)."""
    log("execute query: \n" + the_query)
    sparqlQuery.setQuery(the_query)
    return sparqlQuery.query().convert()


def my_update(the_query):
    """Execute the given update SPARQL query on the tripple store,
    if the given query is no update query, nothing happens."""
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        sparqlUpdate.query()


start_loop()
