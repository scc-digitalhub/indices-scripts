import datetime
import io
import os
import sys
import time

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

sys.path.append('.')
sys.path.append('..')

BATCH_ACCOUNT_NAME = ''
BATCH_ACCOUNT_KEY = ''
BATCH_ACCOUNT_URL = ''
STORAGE_ACCOUNT_NAME = ''
STORAGE_ACCOUNT_KEY = ''
POOL_ID = 'WikiSnapBatch'
DEDICATED_POOL_NODE_COUNT = 0
LOW_PRIORITY_POOL_NODE_COUNT = 0
POOL_VM_SIZE = 'Standard_D2_v3'
JOB_ID = 'WikiSnapBatch'

FILELIST = [
#
# Here goes dumps file list
#
]

SNAPLIST = [
#
# Here goes snapshots file list
# 
]



def query_yes_no(question, default="yes"):
    """
    Prompts the user for yes/no input, displaying the specified question text.

    :param str question: The text of the prompt for input.
    :param str default: The default if the user hits <ENTER>. Acceptable values
    are 'yes', 'no', and None.
    :rtype: str
    :return: 'yes' or 'no'
    """
    valid = {'y': 'yes', 'n': 'no'}
    if default is None:
        prompt = ' [y/n] '
    elif default == 'yes':
        prompt = ' [Y/n] '
    elif default == 'no':
        prompt = ' [y/N] '
    else:
        raise ValueError("Invalid default answer: '{}'".format(default))

    while 1:
        choice = input(question + prompt).lower()
        if default and not choice:
            return default
        try:
            return valid[choice[0]]
        except (KeyError, IndexError):
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def print_batch_exception(batch_exception):
    """
    Prints the contents of the specified Batch exception.

    :param batch_exception:
    """
    print('-------------------------------------------')
    print('Exception encountered:')
    if batch_exception.error and \
            batch_exception.error.message and \
            batch_exception.error.message.value:
        print(batch_exception.error.message.value)
        if batch_exception.error.values:
            print()
            for mesg in batch_exception.error.values:
                print('{}:\t{}'.format(mesg.key, mesg.value))
    print('-------------------------------------------')


def read_from_container(blob_name, block_blob_client, container_name):
    """
    Read a file from Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str blob_name: The file's key.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """

    # Obtain the SAS token for the container.
    sas_token = get_container_sas_token(block_blob_client,
                                        container_name, azureblob.BlobPermissions.READ)

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=blob_name,
                                    http_url=sas_url)


def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately. Expiration is in 2 hours.
    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(days=10))

    return container_sas_token


def get_container_sas_url(block_blob_client,
                          container_name, blob_permissions):
    """
    Obtains a shared access signature URL that provides write access to the
    ouput container to which the tasks will upload their output.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS URL granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container.
    sas_token = get_container_sas_token(block_blob_client,
                                        container_name, azureblob.BlobPermissions.WRITE)

    # Construct SAS URL for the container
    container_sas_url = "https://{}.blob.core.windows.net/{}?{}".format(
        STORAGE_ACCOUNT_NAME, container_name, sas_token)

    return container_sas_url


def create_pool(batch_service_client, pool_id):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str pool_id: An ID for the new pool.
    :param str publisher: Marketplace image publisher
    :param str offer: Marketplace image offer
    :param str sku: Marketplace image sky
    """
    print('Creating pool [{}]...'.format(pool_id))

    # Create a new pool of Linux compute nodes using an Azure Virtual Machines
    # Marketplace image. For more information about creating pools of Linux
    # nodes, see:
    # https://azure.microsoft.com/documentation/articles/batch-linux-nodes/

    # The start task installs ffmpeg on each node from an available repository, using
    # an administrator user identity.
    # 
    task_commands = [
        "export LC_ALL=C.UTF-8",
        "apt-get update",
        "apt-get -y install build-essential libssl-dev libffi-dev python-dev",
        "apt-get -y install p7zip-full",
        "apt-get -y install git",
        "apt-get -y install python3-pip",
        "git clone https://github.com/CristianCantoro/compressed_stream.git",
        "cd compressed_stream",
        "python3 setup.py install",
        "cd ..",
        "git clone https://github.com/WikiLinkGraphs/wikipedia-graphsnapshot.git",
        "cd wikipedia-graphsnapshot",
        "python3 setup.py install",
        "cd ..",
        "pip3 install networkx",
        "cat /proc/cpuinfo | grep model > cpu.txt"
    ]

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=batchmodels.ImageReference(
                publisher="Canonical",
                offer="UbuntuServer",
                sku="18.04-LTS",
                version="latest"
            ),
            node_agent_sku_id="batch.node.ubuntu 18.04"),
        vm_size=POOL_VM_SIZE,
        target_dedicated_nodes=DEDICATED_POOL_NODE_COUNT,
        target_low_priority_nodes=LOW_PRIORITY_POOL_NODE_COUNT,
        start_task=batchmodels.StartTask(
            command_line=wrap_commands_in_shell('linux',task_commands),
            wait_for_success=True,
            user_identity=batchmodels.UserIdentity(
                auto_user=batchmodels.AutoUserSpecification(
                    scope=batchmodels.AutoUserScope.pool,
                    elevation_level=batchmodels.ElevationLevel.admin)),
        )
    )

    batch_service_client.pool.add(new_pool)


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """
    print('Creating job [{}]...'.format(job_id))

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    batch_service_client.job.add(job)


def add_tasks(batch_service_client, job_id, input_files, output_files):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """
    output_container_sas_url_f,output_container_sas_url_s = output_files[0], output_files[1]
    
    print('Adding {} tasks to job [{}]...'.format(len(input_files), job_id))

    tasks = list()

    for idx, couple in enumerate(input_files):
        
        snap = couple[1]
        snap_file_path = snap.file_path
        year = snap_file_path.split("/")[-1]
        year = year.replace("snapshot.","").replace("-05-01.csv.gz","")
        
        rawlink_file = couple[0]
        rawlink_file_path = rawlink_file.file_path
        filename = rawlink_file_path.split("/")[-1]
        
        
        SUFFIX_FEATS = f".features.{year}-05-01.csv.gz"
        SUFFIX_STATS = f".stats.{year}-05-01.xml.gz"
        output_file_path_f = filename+SUFFIX_FEATS
        output_file_path_s = filename+SUFFIX_STATS
        
        command = f"/bin/bash -c \"graphsnapshot --output-compression gzip {rawlink_file_path} . extract-link-snapshot --date {year}-05-01 --snapshot-file {snap_file_path} --skip-snapshot-header && rm {rawlink_file_path} {snap_file_path}\""
        tasks.append(batch.models.TaskAddParameter(
            id='Task{}'.format(idx),
            command_line=command,
            resource_files=[rawlink_file, snap],
            output_files=[
                batchmodels.OutputFile(
                    file_pattern=output_file_path_f,
                    destination=batchmodels.OutputFileDestination(
                            container=batchmodels.OutputFileBlobContainerDestination(
                                container_url=output_container_sas_url_f,
                                path="rawwikilinks-snapshot/2020-05-01/enwiki/"+output_file_path_f)),
                    upload_options=batchmodels.OutputFileUploadOptions(
                        upload_condition=batchmodels.OutputFileUploadCondition.task_success)),
                batchmodels.OutputFile(
                    file_pattern=output_file_path_s,
                    destination=batchmodels.OutputFileDestination(
                            container=batchmodels.OutputFileBlobContainerDestination(
                                container_url=output_container_sas_url_s,
                                path="rawwikilinks-snapshot/2020-05-01/enwiki/"+output_file_path_s)),
                    upload_options=batchmodels.OutputFileUploadOptions(
                        upload_condition=batchmodels.OutputFileUploadCondition.task_success))]
            )
        )
    batch_service_client.task.add_collection(job_id, tasks)


def wait_for_tasks_to_complete(batch_service_client, job_id):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The id of the job whose tasks should be monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """

    print("Monitoring all tasks for 'Completed' state")
    while 1:
        print('.', end='')
        sys.stdout.flush()
        tasks = batch_service_client.task.list(job_id)

        incomplete_tasks = [task for task in tasks if
                            task.state != batchmodels.TaskState.completed]
        if not incomplete_tasks:
            print()
            return True
        else:
            time.sleep(1)


def wrap_commands_in_shell(ostype, commands):
    """Wrap commands in a shell
    :param list commands: list of commands to wrap
    :param str ostype: OS type, linux or windows
    :rtype: str
    :return: a shell wrapping commands
    """
    if ostype.lower() == 'linux':
        return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
            ';'.join(commands))
    elif ostype.lower() == 'windows':
        return 'cmd.exe /c "{}"'.format('&'.join(commands))
    else:
        raise ValueError('unknown ostype: {}'.format(ostype))


if __name__ == '__main__':

    start_time = datetime.datetime.now().replace(microsecond=0)
    print('Sample start: {}'.format(start_time))
    print()

    # Create the blob client, for use in obtaining references to
    # blob storage containers and uploading files to containers.

    blob_client = azureblob.BlockBlobService(
        account_name=STORAGE_ACCOUNT_NAME,
        account_key=STORAGE_ACCOUNT_KEY)

    # Use the blob client to create the containers in Azure Storage if they
    # don't yet exist.

    input_container_name = 'wiki'
    output_container_name_f = 'wiki'
    output_container_name_s = "stats"

    
    rawlinks_input_file_paths = ["rawwikilinks/2020-05-01/enwiki/"+file+".features.xml.gz" for file in FILELIST]
    rawlinks_input_files = [
        read_from_container(file_path,blob_client, input_container_name)
        for file_path in rawlinks_input_file_paths]

    snap_input_file_paths = ["snapshot/2020-05-01/enwiki/"+file for file in SNAPLIST]
    snap_input_files = [
        read_from_container(file_path,blob_client, input_container_name)
        for file_path in snap_input_file_paths]

    input_files = list()
    for i in snap_input_files:
        for j in rawlinks_input_files:
            input_files.append([j,i])
    

    # Obtain a shared access signature URL that provides write access to the output
    # container to which the tasks will upload their output.

    output_container_sas_url_f = get_container_sas_url(
        blob_client,
        output_container_name_f,
        azureblob.BlobPermissions.WRITE)
    
    output_container_sas_url_s = get_container_sas_url(
        blob_client,
        output_container_name_s,
        azureblob.BlobPermissions.WRITE)

    output_containers = [output_container_sas_url_f,output_container_sas_url_s]

    # Create a Batch service client. We'll now be interacting with the Batch
    # service in addition to Storage
    credentials = batchauth.SharedKeyCredentials(BATCH_ACCOUNT_NAME,
                                                 BATCH_ACCOUNT_KEY)

    batch_client = batch.BatchServiceClient(
        credentials,
        batch_url=BATCH_ACCOUNT_URL)

    try:
        # Create the pool that will contain the compute nodes that will execute the
        # tasks.
        create_pool(batch_client, POOL_ID)

        # Create the job that will run the tasks.
        create_job(batch_client, JOB_ID, POOL_ID)

        # Add the tasks to the job. Pass the input files and a SAS URL
        # to the storage container for output files.
        add_tasks(batch_client, JOB_ID,input_files, output_containers)

        # Pause execution until tasks reach Completed state.
        wait_for_tasks_to_complete(batch_client,JOB_ID)


        print("  Success! All tasks reached the 'Completed' state within the "
              "specified timeout period.")

    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise

    finally:

        # Print out some timing info
        end_time = datetime.datetime.now().replace(microsecond=0)
        print()
        print('Sample end: {}'.format(end_time))
        print('Elapsed time: {}'.format(end_time - start_time))
        print()

        # Clean up Batch resources (if the user so chooses).
        if query_yes_no('Delete job?') == 'yes':
            batch_client.job.delete(JOB_ID)

        if query_yes_no('Delete pool?') == 'yes':
            batch_client.pool.delete(POOL_ID)

        print()
        input('Press ENTER to exit...')
