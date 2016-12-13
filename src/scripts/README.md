# Scripts

Collection of helper or convenience scripts.

* `run_on_cluster.sh`<br>
Convenience script for running the indexing process with different corpus configurations on the Betaweb cluster.

* `elasticsearch`
    * `templates`<br>
    Various Elasticsearch indexing templates describing the layout of a Chatnoir2 index

    * `Elasticsearch.postman_collection`<br>
    Helper snippets for use with the [Postman](https://www.getpostman.com/) REST client. These snippets contain
    useful Elasticsearch API calls so you don't have to look them up all the time and run them via `curl`.<br>
    For using these snippets, define an environment with `{{eshost}}` and `{{index}}` containing the hostname of your
    Elasticsearch cluster entry node and the index name.