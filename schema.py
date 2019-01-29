#!/usr/bin/env python
'''
A helper class that can connect schema registry URL and will get the schema ID.
Based on the given Server name , topic name , port number , this is class will give the schema_id of the respective Topic. 
'''
import requests

def getting_value_schema(var_cassandra_conn_host, var_topic_name,var_schema_url_port):
    var_http ='http://'
    var_vers = '/versions/1'
    var_subj = '/subjects/'
    var_topic_val = var_topic_name + '-value'
    var_url = var_http+var_cassandra_conn_host+':'+var_schema_url_port+var_subj+var_topic_val+var_vers
    print(var_url)
    r= requests.get(var_url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return "Error: " + str(e)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)

    b = r.json()
    c=b.get('schema')
    return c


def getting_key_schema(var_cassandra_conn_host, var_topic_name,var_schema_url_port):
    var_http ='http://'
    var_vers = '/versions/1'
    var_subj = '/subjects/'
    var_topic_val = var_topic_name + '-key'
    var_url = var_http+var_cassandra_conn_host+':'+var_schema_url_port+var_subj+var_topic_val+var_vers
    r= requests.get(var_url)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return "Error: " + str(e)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else",err)

    b = r.json()
    c=b.get('schema')
    return c

