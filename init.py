"""
Create Synapse resources for the project which will model exporter 3.0 data
"""
import synapseclient
import boto3
import argparse

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
    args = parser.parse_args()
    return(args)

def copy_data(syn, source_project, target_folder, exclude_tables=None,
              query_str=None, file_handle_field="rawData"):
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
            file_handle_field = file_handle_field)

def copy_data_(syn, s3_client, source_table, data_folder, query_str, file_handle_field):
    if query_str is None:
        query_str = f"SELECT * FROM {source_table}"
    else:
        query_str = query_str.format(source_table = source_table)
    print(query_str)
    q = syn.tableQuery(query_str)
    file_handles = syn.downloadTableColumns(q, file_handle_field)
    source_df = q.asDataFrame()
    source_df['path'] = source_df[file_handle_field].astype(str).map(file_handles)
    for _, r in source_df.iterrows():
        annotations = {
                "recordId": str(r["recordId"]),
                "healthCode": str(r["healthCode"]),
                "createdOn": str(r["createdOn"]),
                "taskIdentifier": str(r["metadata.taskIdentifier"]),
                "substudyMemberships": str(r["substudyMemberships"])}
        f = synapseclient.File(
            path = r["path"],
            parent = data_folder,
            annotations = annotations)
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


def main():
    args = read_args()
    syn = synapseclient.login()
    copy_data(
        syn = syn,
        source_project = args.source_project,
        target_folder = args.data_folder,
        exclude_tables = args.exclude_tables,
        query_str = args.query_str,
        file_handle_field = args.file_handle_field)

if __name__ == "__main__":
    main()
