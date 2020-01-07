from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


def df_load_function(file, context):
    print("Processing file: {filename}".format(filename=file['name']))
    
    credentials = GoogleCredentials.get_application_default()
    
    # Set your project name.
    project = 'project'
    
    # Set bucket path e.g. gs://project
    inputfile = 'bucket path' + file['name']

    # Files need to be processed with dataflow.
    filesnames = [
        'file1',
        'file2']

    for i in filesnames:
        
        # Checking file in folders. 
        if 'folder1/inside_folder/{}'.format(i) in file['name']:
            # Set Dataflow job name.
            job = 'dataflowjob_{}'.format(i)
            
            # Set path to dataflow template. In this example template required parameters based on how template is created.
            template = 'gs://project/template/dataflow_template_{}'.format(i)
            location = 'europe-west1'
            parameters = {
                'filepath': inputfile
            }

            # Build dataflow request.
            dataflow = build('dataflow', 'v1b3', credentials=credentials)
            request = dataflow.projects().locations().templates().launch(
                projectId=project,
                gcsPath=template,
                location=location,
                body={
                    'jobName': job,
                    'parameters': parameters
                }
            )

            # Execute and print the request output to stackdriver logging.
            print(request.execute())
