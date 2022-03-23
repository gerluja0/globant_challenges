
from datetime import datetime
from flask import jsonify
from google.cloud import storage
from google.cloud import bigquery
import os



def cargar_csv(request):
    try:
        if request.args and 'csv_name' in request.args:
            csv_name = request.args.get('csv_name')
        else:
            return 'Error en respuesta, verificar ruta loadcsv/csv_name', 412

        client_cs = storage.Client()
        client = bigquery.Client()
        dataset_id = 'globant'
        dataset_ref = client.dataset(dataset_id)

        found_csv = ''
        for blob in client_cs.list_blobs(os.environ['BUCKET']):
            if str(blob).split(',')[1].strip()==csv_name:
                found_csv=str(blob).split(',')[1].strip()

        if csv_name==found_csv:
            job_config = bigquery.LoadJobConfig()
            job_config.skip_leading_rows = 1
            job_config.field_delimiter = ';',
            job_config.source_format = bigquery.SourceFormat.CSV

            if found_csv=='hired_employees.csv':
                job_config.schema = [
                    bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
                    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('datetime', 'STRING', mode='REQUIRED'),
                    bigquery.SchemaField('department_id', 'INTEGER', mode='REQUIRED'),
                    bigquery.SchemaField('job_id', 'INTEGER', mode='REQUIRED')
                ]

                table_name=found_csv.split('.',1)[0]
                uri = 'gs://' + os.environ['BUCKET'] + '/' + found_csv
            
            elif found_csv=='departments.csv':
                job_config.schema = [
                    bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
                    bigquery.SchemaField('department', 'STRING', mode='REQUIRED')
                ]

                table_name=found_csv.split('.',1)[0]
                uri = 'gs://' + os.environ['BUCKET'] + '/' + found_csv

            elif found_csv=='jobs.csv':
                job_config.schema = [
                    bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
                    bigquery.SchemaField('job', 'STRING', mode='REQUIRED')
                ]

                table_name=found_csv.split('.',1)[0]
                uri = 'gs://' + os.environ['BUCKET'] + '/' + found_csv


            load_job = client.load_table_from_uri(
            uri,
            dataset_ref.table(table_name),
            job_config=job_config)
                
            
            response = jsonify({
                'httpResponseCode': '200',
                'errorMessage':'file '+ csv_name +' cargado a Bigquery'
                })
            response.status_code = 200

        else:
            response = jsonify({
                'httpResponseCode': '404',
                'errorMessage':'file '+ csv_name +' no existe'
                })
            response.status_code = 404

            
            rows_to_insert = [
            {'nombre_csv':csv_name , 
             'date': str(datetime.today()).split()[0],
             'texto_error':'file '+ csv_name +' no existe en el Bucket'+', status code:'+str(response.status_code)}]
            
            errors = client.insert_rows_json('project-datatest-001.globant.de_logs', rows_to_insert)  # Make an API request.
            if errors == []:
                print("Fila insertada.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))


        return response



    except Exception as e:
        return str(e)
   