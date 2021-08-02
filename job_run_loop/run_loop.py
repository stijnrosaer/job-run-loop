import json
import os

from .helpers import log, generate_uuid, my_update, my_query
from .escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_int, sparql_escape_datetime
from string import Template
from .file_handler import postfile, get_file_by_id

from time import sleep

MU_APPLICATION_GRAPH = os.environ.get('MU_APPLICATION_GRAPH')


def get_job(task):
    """
    Get the first queued job in the triple store
    :return: job description with source file location, task identifier and job id
    """
    q = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX status: <http://mu.semte.ch/vocabularies/ext/status#>
    SELECT ?source ?task ?uuid
    FROM $graph
    WHERE {
       ?job a <http://mu.semte.ch/vocabularies/ext/Job> ;
          mu:source ?source;
          mu:task $task;
          mu:uuid ?uuid;
          mu:status status:queued . 
    }
    LIMIT 1
    """).substitute(
        task=sparql_escape_string(task),
        graph=sparql_escape_uri(MU_APPLICATION_GRAPH),
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
    status = "http://mu.semte.ch/vocabularies/ext/status#" + status

    q = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/ext/>
    WITH $graph
    DELETE {?job mu:status ?status}
    INSERT {?job mu:status $newstate}
    WHERE {
        ?job a <http://mu.semte.ch/vocabularies/ext/Job> ;
          mu:uuid "$uuid";
          mu:status ?status .
    }
    """).substitute(
        uuid=id,
        newstate=sparql_escape_uri(status),
        graph=sparql_escape_uri(MU_APPLICATION_GRAPH),
    )
    my_update(q)

def add_result(id, result_file_id):
    """
    Forward status of job to the next state. If a state is skipped, a new job will be created!
    :param id: job id
    :param status: new status (queued, processing, done)
    :return:
    """
    q = Template("""
    PREFIX mu: <http://mu.semte.ch/vocabularies/ext/>
    INSERT { GRAPH $graph {?job mu:result "$file_id"} }
    WHERE { GRAPH $graph {
        ?job a <http://mu.semte.ch/vocabularies/ext/Job> ;
          mu:uuid "$uuid" .
    } }
    """).substitute(
        uuid=id,
        graph=sparql_escape_uri(MU_APPLICATION_GRAPH),
        file_id=result_file_id,
    )
    my_update(q)

def start_loop(call_method):
    """
    Run the job loop to fetch jobs and execute them is correct task type.
    Single threaded: only one task is executed per loop
    :param call_method: Method to be called after loading data
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

                if resp:
                    if os.path.isfile(resp):
                        dumpFileName = resp.split("/")[-1]
                        dumpFilePath = resp
                    else:
                        file_id = generate_uuid()
                        dumpFileName = f"{file_id}.json"
                        dumpFilePath = f'/share/ai-files/{dumpFileName}'
                        with open(dumpFilePath, 'w') as f:
                            f.write(resp)

                    file_info = postfile(dumpFilePath, dumpFileName)["id"]
                    add_result(id, file_info)

                update_job(id, "done")

            except Exception as e:
                log(e)
                update_job(id, "failed")

        sleep(20)
