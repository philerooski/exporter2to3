"""
Create Synapse resources for the project which will model exporter 3.0 data
"""
import synapseclient
import boto3
import argparse
from datetime import datetime

def read_args():
    parser = argparse.ArgumentParser(
        description=("Copy data in Bridge exporter 2.0 format to a new "
                     "project in Bridge exporter 3.0 format."))
    parser.add_argument(
            "--source-project",
            help="Synapse ID of the project where data is being sourced.")
    parser.add_argument(
            "--data-folder",
            help="Synapse ID of the folder where data will be exported to.")
    parser.add_argument(
            "--exclude-tables",
            action="extend",
            nargs="+",
            type=str,
            help="Synapse ID of the folder where data will be exported to.")
    parser.add_argument(
            "--query-str",
            help=("An f-string formatted query string to pull filtered data from "
                  "tables in the source project. Use {source_table} "
                  "in FROM clause."))
    parser.add_argument(
            "--file-handle-field",
            help=("The field name in the source tables containing the raw "
                  "data archive."),
            default="rawData")
    parser.add_argument(
            "--dry-run",
            help="Do not move any data.")
    args = parser.parse_args()
    return(args)

def copy_data(syn, source_project, target_folder, exclude_tables=None,
              query_str=None, file_handle_field="rawData", dry_run=False):
    source_tables = syn.getChildren(source_project, includeTypes=["table"])
    s3_client = boto3.client("s3")
    for table in source_tables:
        if table["id"] in exclude_tables:
            continue
        copy_data_(
            syn = syn,
            s3_client = s3_client,
            source_table = table["id"],
            data_folder = target_folder,
            query_str = query_str,
            file_handle_field = file_handle_field,
            dry_run=dry_run)


def create_new_parent_folder(syn, data_folder, partition_num):
    folder = synapseclient.Folder(
            name=f"partition_{partition_num}",
            parent=data_folder)
    folder = syn.store(folder)
    return folder["id"]

def copy_data_(syn, s3_client, source_table, data_folder,
        query_str, file_handle_field, dry_run):
    if query_str is None:
        query_str = f"SELECT * FROM {source_table}"
    else:
        query_str = query_str.format(source_table = source_table)
    print(query_str)
    q = syn.tableQuery(query_str)
    file_handles = syn.downloadTableColumns(q, file_handle_field)
    source_df = q.asDataFrame()
    source_df['path'] = source_df[file_handle_field].astype(str).map(file_handles)
    counter = 0
    for _, r in source_df.iterrows():
        if counter % 9999 == 0:
            parent_folder = create_new_parent_folder(
                    syn=syn,
                    data_folder=data_folder,
                    partition_num=counter//9999)
        dt_format_str = "%Y-%m-%dT%H:%M:%S.000Z"
        uploaded_on = datetime.fromtimestamp( # uploadDate is not precise enough
                r["createdOn"]/1000).strftime(dt_format_str)
        annotations = {
                "recordId": str(r["recordId"]),
                "phoneInfo": str(r["phoneInfo"]),
                "appVersion": str(r["appVersion"]),
                "healthCode": str(r["healthCode"]),
                "createdOn": str(r["createdOn"]),
                "createdOnTimeZone": str(r["createdOnTimeZone"]),
                "dayInStudy": str(r["dayInStudy"]),
                "taskIdentifier": str(r["metadata.taskIdentifier"]),
                "assessmentId": str(r["metadata.taskIdentifier"]),
                "dataType": str(r["dataType"]),
                "substudyMemberships": str(r["substudyMemberships"]),
                "dataGroups": str(r["dataGroups"]),
                "uploadedOn": uploaded_on,
                "appVersion": str(r["appVersion"])}
        f = synapseclient.File(
            path = r["path"],
            parent = parent_folder,
            annotations = annotations)
        if not dry_run:
            f = syn.store(f)
            # write metadata to S3 object
            copy_source = {
                    "Bucket": f["_file_handle"]["bucketName"],
                    "Key": f["_file_handle"]["key"]}
            s3_client.copy_object(
                    CopySource = copy_source,
                    Bucket = copy_source["Bucket"],
                    Key = copy_source["Key"],
                    Metadata = annotations,
                    MetadataDirective="REPLACE")
        counter += 1


def main():
    args = read_args()
    syn = synapseclient.login()
    copy_data(
        syn = syn,
        source_project = args.source_project,
        target_folder = args.data_folder,
        exclude_tables = args.exclude_tables,
        query_str = args.query_str,
        file_handle_field = args.file_handle_field
        dry_run = args.dry_run)

if __name__ == "__main__":
    main()
