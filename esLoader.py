#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import re
from itertools import chain

import click
import os
import elasticsearch
from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import NotFoundError
from pkg_resources import iter_entry_points

from click_conf import conf
from click_stream import Stream

from package.iter import bulk_builder, grouper, json_lines_iter
from package.parsers import csv, json, parquet, pd
from package.logger import log

@click.group(invoke_without_command=True, context_settings={"help_option_names": ['-h', '--help']})
@conf(default='esl.yml')
@click.option('--es-host', default=['http://localhost:9200'], multiple=True, envvar='ES_HOST',
              help='Elasticsearch cluster entry point. (default http://localhost:9200)')
@click.option('--index', help='Destination index name', required=True)
@click.option('--type', help='Docs type. TYPES WILL BE DEPRECATED IN APIS IN ELASTICSEARCH 7, AND COMPLETELY REMOVED IN 8.', required=True, default='_doc')
@click.option('--bulk-size', default=500, help='How many docs to collect before writing to Elasticsearch (default 500)')
@click.option('--verify-certs', default=False, is_flag=True, help='Make sure we verify SSL certificates (default false)')
@click.option('--use-ssl', default=False, is_flag=True, help='Turn on SSL (default false)')
@click.option('--ca-certs', help='Provide a path to CA certs on disk')
@click.option('--http-auth', help='Provide username and password for basic auth in the format of username:password')
@click.option('--delete', default=False, is_flag=True, help='Delete index before import? (default false)')
@click.option('--update', default=False, is_flag=True, help='Merge and update existing doc instead of overwrite')
@click.option('--progress', default=False, is_flag=True, help='Enable progress bar - '
              'NOTICE: in order to show progress the entire input should be collected and can consume more memory than without progress bar')
@click.option('--id_field', help='Specify field name that be used as document id')
@click.option('--as_child', default=False, is_flag=True, help='Insert _parent, _routing field, '
              'the value is same as _id. Note: must specify --id-field explicitly')
@click.option('--parent_id_field', help="Parent ID", required=False)
@click.option('--with-retry', default=False, is_flag=True, help='Retry if ES bulk insertion failed')
@click.option('--index_settings_file', type=click.File('rb'), help='Specify path to json file containing index mapping and settings, creates index if missing')
@click.option('--timeout', type=float, help='Specify request timeout in seconds for Elasticsearch client', default=10)
@click.option('--encoding', type=str, help='Specify content encoding for input files', default='utf-8')
@click.option('--keys', type=str, help='Comma separated keys to pick from each document', default='', callback=lambda c, p, v: [x for x in v.split(',') if x])
@click.option('--pipeline', type=str, help='Specify a pipeline to be applied for each document')
@click.pass_context
def cli(ctx, **opts):
    ctx.obj = opts
    log('info', ctx.obj)
    es_opts = {x: y for x, y in opts.items() if x in ('timeout', 'use_ssl', 'ca_certs', 'verify_certs', 'http_auth')}
    ctx.obj['es_conn'] = Elasticsearch(opts['es_host'], **es_opts)
    if opts['delete']:
        try:
            ctx.obj['es_conn'].indices.delete(opts['index'])
            log('info', 'Index %s deleted' % opts['index'])
            return
        except NotFoundError:
            log('info', 'Skipping index deletion')
            return
    if opts['index_settings_file']:
        if ctx.obj['es_conn'].indices.exists(index=opts['index']):
            pass
        else:
            # Elasticsearch.indices.create(index='', include_type_name=True)
            ctx.obj['es_conn'].indices.create(
                index=opts['index'], body=json.load(opts['index_settings_file']), include_type_name=True)
    if ctx.invoked_subcommand is None:
        commands = cli.commands.keys()
        if ctx.default_map:
            default_command = ctx.default_map.get('default_command')
            if default_command:
                command = cli.get_command(ctx, default_command)
                if command:
                    ctx.invoke(command, **ctx.default_map[default_command]['arguments'])
                    return
                else:
                    ctx.fail('Cannot find default_command: {},\navailable commands are: {}'.format(default_command, ", ".join(commands)))
            else:
                ctx.fail('No subcommand specified via command line / task file,\navailable commands are: {}'.format(", ".join(commands)))
        else:
            ctx.fail('No subcommand specified via command line / task file,\navailable commands are: {}'.format(", ".join(commands)))


@cli.command(name='csv')
# @click.argument('files', type=Stream(file_mode='rU'), nargs=-1, required=True)
@click.argument('filename', type=click.Path(exists=True), nargs=1, required=True)
@click.option('--delimiter', default=',', type=str, help='Default ,')
@click.pass_context
def _csv(ctx, filename, delimiter):
    if delimiter == '\\t':
        delimiter = '\t'
    
    log('info', filename)
    csvFile = open(os.path.abspath(filename), 'r')
    # lines = chain(*(csv.DictReader(x, delimiter=str(delimiter)) for x in files))
    df = pd.read_csv(csvFile)
    df.columns = df.columns.str.upper()
    lines = df.to_json(orient='records')

    regex = r'(\w+).csv'
    match = re.search(regex, filename)
    if match:
        keyName = match.group().split('.csv')[0]
        log('info', keyName)
        ctx.obj['keyName'] = keyName

    jsons = json.loads(lines)      
    load(jsons, ctx.obj)


@cli.command(name='json', short_help='FILES with the format of [{"a": "1"}, {"b": "2"}]')
@click.argument('files', type=Stream(file_mode='r'), nargs=-1, required=True)
@click.option('--lines', '--json-lines', default=False, is_flag=True, help='Files formatted as json lines')
@click.pass_context
def _json(ctx, files, lines):
    if lines:
        lines = chain(*(json_lines_iter(x) for x in files))
    else:
        try:
            lines = chain(*(json.load(x) for x in files))
        except Exception:
            log('error', 'Got serialization error while trying to decode json, if you trying to load json lines add the --lines flag')
            raise
    load(lines, ctx.obj)


@cli.command(name='parquet')
# @click.argument('files', type=Stream(file_mode='rb', encoding='utf8'), nargs=-1, required=True)
@click.argument('filename', type=click.Path(exists=True), nargs=1, required=True)
@click.pass_context
def _parquet(ctx, filename):
    if not parquet:
        raise SystemExit("parquet module not found, please run 'pip install parquet'")
    if not pd:
        raise SystemExit("parquet module not found, please run 'pip install pandas'")
    log('info', filename)
    
    parquetFile = open(os.path.abspath(filename), 'r')
    # labels = ['ID', 'CREATED_AT', 'UPDATED_AT']
    # headers = list(pd.read_parquet(parquetFile, engine='pyarrow').columns.values)
    # lines = list(parquet.DictReader(parquetFile, columns=labels))
    lines = pd.read_parquet(parquetFile, engine='pyarrow').to_json(orient='records')

    regex = r'(\w+).parquet'
    match = re.search(regex, filename)
    if match:
        keyName = match.group().split('.parquet')[0]
        log('info', keyName)
        ctx.obj['keyName'] = keyName

    jsons = json.loads(lines)      
    load(jsons, ctx.obj)

def single_bulk_to_es(bulk, config, attempt_retry):
    with open('./config/config.json', 'r') as f:
        configFile = json.load(f)
    config['encryption_key'] = configFile['DEFAULT']['ENCRYPTION_KEY'] 
    bulk = bulk_builder(bulk, config)
    # log('info', '\n'.join(str(x) for x in bulk))

    max_attempt = 1
    if attempt_retry:
        max_attempt += 3
    for attempt in range(1, max_attempt + 1):
        try:
            helpers.bulk(config['es_conn'], bulk, chunk_size=config['bulk_size'])
        except Exception as e:
            if attempt < max_attempt:
                wait_seconds = attempt * 3
                log('warn', 'attempt [%s/%s] got exception, will retry after %s seconds' % (attempt, max_attempt, wait_seconds))
                time.sleep(wait_seconds)
                continue

            log('error', 'attempt [%s/%s] got exception, it is a permanent data loss, no retry any more' % (attempt, max_attempt))
            raise e

        if attempt > 1:
            log('info', 'attempt [%s/%s] succeed. We just get recovered from previous error' % (attempt, max_attempt))
        break


def load(lines, config):
    bulks = grouper(lines, config['bulk_size'] * 3)
    # bulks = lines
    log('info', config)
    if config['progress']:
        bulks = [x for x in bulks]
    with click.progressbar(bulks) as pbar:
        for i, bulk in enumerate(pbar):
            try:   
                single_bulk_to_es(bulk, config, config['with_retry'])
            except Exception as e:
                log('warn', 'Chunk {i} got exception ({e}) while processing'.format(e=e, i=i))
                raise

def load_plugins():
    for plugin in iter_entry_points(group='esl.plugins'):
        name, entry = plugin.resolve()()
        cli.command(name=name)(entry)


load_plugins()

if __name__ == '__main__':
    cli()